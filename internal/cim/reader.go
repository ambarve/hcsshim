package cim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Microsoft/hcsshim/internal/cim/format"
)

var (
	// ErrFileNotFound indicates that a file with the requested path was not
	// found.
	ErrFileNotFound = errors.New("no such file or directory")
	// ErrNotADirectory indicates that a directory operation was attempted on a
	// non-directory file.
	ErrNotADirectory = errors.New("not a directory")
	// ErrIsADirectory indicates that a non-directory operation was attempted on
	// a directory.
	ErrIsADirectory = errors.New("is a directory")
)

type region struct {
	f    *os.File
	size int64
}

type fileTable []byte

// A Reader is a CIM file opened for read access.
type Reader struct {
	name       string
	reg        []region
	ftdes      []format.FileTableDirectoryEntry
	ftables    []fileTable
	upcase     []uint16
	root       *inode
	cm         sync.Mutex
	inodeCache map[format.FileID]*inode
	sdCache    map[format.RegionOffset][]byte
}

// A File is a file or directory within an open CIM.
type File struct {
	r    *Reader
	name string
	sr   streamReader
	ino  *inode
}

type inode struct {
	id          format.FileID
	file        format.File
	linkTable   []byte
	streamTable []byte
}

type streamReader struct {
	stream     format.Stream
	off        int64
	pe         format.PeImage
	pemappings []format.PeImageMapping
	peinit     bool
}

// A Stream is an alternate data stream of a file within a CIM.
type Stream struct {
	c     *Reader
	r     streamReader
	fname string
	name  string
}

// ReaderError is the error type returned by most functions in this package.
type ReaderError struct {
	Cim    string
	Op     string
	Path   string
	Stream string
	Err    error
}

func (e *ReaderError) Error() string {
	s := "cim " + e.Op + " " + e.Cim
	if e.Path != "" {
		s += ":" + e.Path
		if e.Stream != "" {
			s += ":" + e.Stream
		}
	}
	s += ": " + e.Err.Error()
	return s
}

func readBin(r io.Reader, v interface{}) error {
	err := binary.Read(r, binary.LittleEndian, v)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

func validateHeader(h *format.CommonHeader) error {
	if !bytes.Equal(h.Magic[:], format.MagicValue[:]) {
		return errors.New("not a cim file")
	}
	if h.Version.Major != format.CurrentVersion.Major {
		return fmt.Errorf("unsupported cim version %v", h.Version)
	}
	return nil
}

func loadRegionSet(rs *format.RegionSet, imagePath string, reg []region) (int, error) {
	for i := 0; i < int(rs.Count); i++ {
		name := fmt.Sprintf("region_%v_%d", rs.ID, i)
		rf, err := os.Open(filepath.Join(imagePath, name))
		if err != nil {
			return 0, err
		}
		reg[i].f = rf
		fi, err := rf.Stat()
		if err != nil {
			return 0, err
		}
		reg[i].size = fi.Size()
		var rh format.RegionHeader
		err = readBin(rf, &rh)
		if err != nil {
			return 0, fmt.Errorf("reading region header %s: %s", name, err)
		}
		err = validateHeader(&rh.Common)
		if err != nil {
			return 0, fmt.Errorf("validating region header %s: %s", name, err)
		}
	}
	return int(rs.Count), nil
}

// Open opens a CIM file for read access.
func Open(p string) (_ *Reader, err error) {
	defer func() {
		if err != nil {
			err = &ReaderError{Cim: p, Op: "open", Err: err}
		}
	}()
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var fsh format.FilesystemHeader
	err = readBin(f, &fsh)
	if err != nil {
		return nil, fmt.Errorf("reading filesystem header: %s", err)
	}
	err = validateHeader(&fsh.Common)
	if err != nil {
		return nil, fmt.Errorf("validating filesystem header: %s", err)
	}
	parents := make([]format.RegionSet, fsh.ParentCount)
	err = readBin(f, parents)
	if err != nil {
		return nil, fmt.Errorf("reading parent region sets: %s", err)
	}
	regionCount := int(fsh.Regions.Count)
	for i := range parents {
		regionCount += int(parents[i].Count)
	}
	if regionCount == 0 || regionCount > 0x10000 {
		return nil, fmt.Errorf("invalid region count %d", regionCount)
	}
	c := &Reader{
		name:       p,
		reg:        make([]region, regionCount),
		upcase:     make([]uint16, format.UpcaseTableLength),
		inodeCache: make(map[format.FileID]*inode),
		sdCache:    make(map[format.RegionOffset][]byte),
	}
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	imagePath := filepath.Dir(p)
	reg := c.reg
	for i := range parents {
		n, err := loadRegionSet(&parents[i], imagePath, reg)
		if err != nil {
			return nil, err
		}
		reg = reg[n:]
	}
	_, err = loadRegionSet(&fsh.Regions, imagePath, reg)
	if err != nil {
		return nil, err
	}

	var fs format.Filesystem
	err = c.readBin(&fs, fsh.FilesystemOffset, 0)
	if err != nil {
		return nil, fmt.Errorf("reading filesystem info: %s", err)
	}
	c.ftables = make([]fileTable, fs.FileTableDirectoryLength)
	c.ftdes = make([]format.FileTableDirectoryEntry, fs.FileTableDirectoryLength)
	err = c.readBin(c.ftdes, fs.FileTableDirectoryOffset, 0)
	if err != nil {
		return nil, fmt.Errorf("reading file table directory: %s", err)
	}
	err = c.readBin(c.upcase, fs.UpcaseTableOffset, 0)
	if err != nil {
		return nil, fmt.Errorf("reading upcase table: %s", err)
	}
	c.root, err = c.getInode(fs.RootDirectory)
	if err != nil {
		return nil, fmt.Errorf("reading root directory: %s", err)
	}
	return c, nil
}

// Close releases resources associated with the Cim.
func (cr *Reader) Close() error {
	for i := range cr.reg {
		cr.reg[i].f.Close()
	}
	return nil
}

func (cr *Reader) objReader(o format.RegionOffset, off, size int64) (*io.SectionReader, error) {
	oi := int(o.RegionIndex())
	ob := o.ByteOffset()
	if oi >= len(cr.reg) || ob == 0 {
		return nil, fmt.Errorf("invalid region offset 0x%x", o)
	}
	reg := cr.reg[oi]
	if ob > reg.size || off > reg.size-ob {
		return nil, fmt.Errorf("%s: invalid region offset 0x%x", reg.f.Name(), o)
	}
	maxsize := reg.size - ob - off
	if size < 0 {
		size = maxsize
	} else if size > maxsize {
		return nil, fmt.Errorf("%s: invalid region size %x at offset 0x%x", reg.f.Name(), size, o)
	}
	return io.NewSectionReader(reg.f, ob+off, size), nil
}

func (cr *Reader) readCounted(o format.RegionOffset, csize int) ([]byte, error) {
	r, err := cr.objReader(o, 0, -1)
	if err != nil {
		return nil, err
	}
	var n uint32
	if csize == 2 {
		var n16 uint16
		err = readBin(r, &n16)
		if err != nil {
			return nil, err
		}
		n = uint32(n16)
	} else if csize == 4 {
		var n32 uint32
		err = readBin(r, &n32)
		if err != nil {
			return nil, err
		}
		n = n32
	} else {
		panic("invalid count size")
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (cr *Reader) readOffsetFull(b []byte, o format.RegionOffset, off int64) (int, error) {
	r, err := cr.objReader(o, off, int64(len(b)))
	if err != nil {
		return 0, err
	}
	return io.ReadFull(r, b)
}

func (cr *Reader) readBin(v interface{}, o format.RegionOffset, off int64) error {
	r, err := cr.objReader(o, off, int64(binary.Size(v)))
	if err != nil {
		return err
	}
	return readBin(r, v)
}

// OpenAt returns a file associated with path `p`, relative to `dirf`. If `dirf`
// is nil or `p` starts with '/', then the path will be opened relative to the
// CIM root.
func (cr *Reader) OpenAt(dirf *File, p string) (_ *File, err error) {
	fullp := p
	defer func() {
		if err != nil {
			err = &ReaderError{Cim: cr.name, Path: fullp, Op: "openat", Err: err}
		}
	}()
	dirOnly := len(p) > 0 && p[len(p)-1] == '/'
	p = path.Clean(p)
	if dirf != nil && !dirf.IsDir() {
		return nil, ErrNotADirectory
	}
	var ino *inode
	if p[0] == '/' {
		ino = cr.root
		p = p[1:]
	} else if dirf == nil {
		ino = cr.root
	} else {
		fullp = path.Join(dirf.name, fullp)
		if dirOnly {
			fullp += "/"
		}
		ino = dirf.ino
	}
	if len(p) > 0 && p != "." {
		for _, name := range strings.Split(p, "/") {
			fid, err := cr.findChild(ino, name)
			if err != nil {
				return nil, ErrFileNotFound
			}
			ino, err = cr.getInode(fid)
			if err != nil {
				return nil, err
			}
		}
	}
	if dirOnly && !ino.IsDir() {
		return nil, ErrNotADirectory
	}
	f := &File{
		r:    cr,
		name: fullp,
		ino:  ino,
		sr:   streamReader{stream: ino.file.DefaultStream},
	}
	return f, nil
}

func (cr *Reader) readFile(id format.FileID, file *format.File) error {
	if id == 0 {
		return fmt.Errorf("invalid file ID %#x", id)
	}
	tid := uint64((id - 1) / format.FilesPerTable)
	tfid := int((id - 1) % format.FilesPerTable)
	if tid >= uint64(len(cr.ftdes)) || tfid >= int(cr.ftdes[tid].Count) {
		return fmt.Errorf("invalid file ID %#x", id)
	}
	esize := int(cr.ftdes[tid].EntrySize)
	if cr.ftables[tid] == nil {
		b := make([]byte, esize*int(cr.ftdes[tid].Count))
		_, err := cr.readOffsetFull(b, cr.ftdes[tid].Offset, 0)
		if err != nil {
			return fmt.Errorf("reading file table %d: %s", tid, err)
		}
		cr.ftables[tid] = b
	}
	// This second copy is needed because the on-disk file size may be smaller
	// than format.File).
	b := make([]byte, binary.Size(file))
	copy(b, cr.ftables[tid][tfid*esize:(tfid+1)*esize])
	readBin(bytes.NewReader(b), file)
	return nil
}

func (cr *Reader) getInode(id format.FileID) (*inode, error) {
	cr.cm.Lock()
	ino, ok := cr.inodeCache[id]
	cr.cm.Unlock()
	if ok {
		return ino, nil
	}
	ino = &inode{
		id: id,
	}
	err := cr.readFile(id, &ino.file)
	if err != nil {
		return nil, err
	}
	switch typ := ino.file.DefaultStream.Type(); typ {
	case format.StreamTypeData,
		format.StreamTypeLinkTable,
		format.StreamTypePeImage:

	default:
		return nil, fmt.Errorf("unsupported stream type: %d", typ)
	}
	cr.cm.Lock()
	cr.inodeCache[id] = ino
	cr.cm.Unlock()
	return ino, nil
}

// Filetime is a Windows FILETIME, in 100-ns units since January 1, 1601.
type Filetime int64

// Time returns a Go time equivalent to `ft`.
func (ft Filetime) Time() time.Time {
	if ft == 0 {
		return time.Time{}
	}
	// 100-nanosecond intervals since January 1, 1601
	nsec := int64(ft)
	// change starting time to the Epoch (00:00:00 UTC, January 1, 1970)
	nsec -= 116444736000000000
	// convert into nanoseconds
	nsec *= 100
	return time.Unix(0, nsec)
}

func (ft Filetime) String() string {
	return ft.Time().String()
}

// A FileInfo specifies information about a file.
type FileInfo struct {
	FileID                                                  uint64
	Size                                                    int64
	Attributes                                              uint32
	ReparseTag                                              uint32
	CreationTime, LastWriteTime, ChangeTime, LastAccessTime Filetime
	SecurityDescriptor                                      []byte
	ExtendedAttributes                                      []byte
	ReparseData                                             []byte
}

// Windows file attributes.
const (
	FILE_ATTRIBUTE_READONLY      = 0x00000001
	FILE_ATTRIBUTE_HIDDEN        = 0x00000002
	FILE_ATTRIBUTE_SYSTEM        = 0x00000004
	FILE_ATTRIBUTE_DIRECTORY     = 0x00000010
	FILE_ATTRIBUTE_ARCHIVE       = 0x00000020
	FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
)

func (ino *inode) IsDir() bool {
	return ino.file.DefaultStream.Type() == format.StreamTypeLinkTable
}

// IsDir returns whether a file is a directory.
func (f *File) IsDir() bool {
	return f.ino.IsDir()
}

func (cr *Reader) getSd(o format.RegionOffset) ([]byte, error) {
	cr.cm.Lock()
	sd, ok := cr.sdCache[o]
	cr.cm.Unlock()
	if ok {
		return sd, nil
	}
	sd, err := cr.readCounted(o, 2)
	if err != nil {
		return nil, fmt.Errorf("reading security descriptor at 0x%x: %s", o, err)
	}
	cr.cm.Lock()
	cr.sdCache[o] = sd
	cr.cm.Unlock()
	return sd, nil
}

func (cr *Reader) stat(ino *inode) (*FileInfo, error) {
	fi := &FileInfo{
		FileID:         uint64(ino.id),
		Size:           ino.file.DefaultStream.Size(),
		ReparseTag:     ino.file.ReparseTag,
		CreationTime:   Filetime(ino.file.CreationTime),
		LastWriteTime:  Filetime(ino.file.LastWriteTime),
		ChangeTime:     Filetime(ino.file.ChangeTime),
		LastAccessTime: Filetime(ino.file.LastAccessTime),
	}
	attr := uint32(0)
	if ino.file.Flags&format.FileFlagReadOnly != 0 {
		attr |= FILE_ATTRIBUTE_READONLY
	}
	if ino.file.Flags&format.FileFlagHidden != 0 {
		attr |= FILE_ATTRIBUTE_HIDDEN
	}
	if ino.file.Flags&format.FileFlagSystem != 0 {
		attr |= FILE_ATTRIBUTE_SYSTEM
	}
	if ino.file.Flags&format.FileFlagArchive != 0 {
		attr |= FILE_ATTRIBUTE_ARCHIVE
	}
	if ino.IsDir() {
		attr |= FILE_ATTRIBUTE_DIRECTORY
	}
	if ino.file.SdOffset != format.NullOffset {
		sd, err := cr.getSd(ino.file.SdOffset)
		if err != nil {
			return nil, err
		}
		fi.SecurityDescriptor = sd
	}
	if ino.file.EaOffset != format.NullOffset {
		b := make([]byte, ino.file.EaLength)
		_, err := cr.readOffsetFull(b, ino.file.EaOffset, 0)
		if err != nil {
			return nil, fmt.Errorf("reading EA buffer at %#x: %s", ino.file.EaOffset, err)
		}
		fi.ExtendedAttributes = b
	}
	if ino.file.ReparseOffset != format.NullOffset {
		b, err := cr.readCounted(ino.file.ReparseOffset, 2)
		if err != nil {
			return nil, fmt.Errorf("reading reparse buffer at %#x: %s", ino.file.EaOffset, err)
		}
		fi.ReparseData = b
		attr |= FILE_ATTRIBUTE_REPARSE_POINT
	}
	fi.Attributes = attr
	return fi, nil
}

// Stat returns a FileInfo for the file.
func (f *File) Stat() (*FileInfo, error) {
	fi, err := f.r.stat(f.ino)
	if err != nil {
		err = &ReaderError{Cim: f.r.name, Path: f.name, Op: "stat", Err: err}
	}
	return fi, err
}

func (cr *Reader) getPESegment(sr *streamReader, off int64) (int64, int64, error) {
	if !sr.peinit {
		err := cr.readBin(&sr.pe, sr.stream.DataOffset, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("reading PE image descriptor: %s", err)
		}
		sr.pe.DataLength &= 0x7fffffffffffffff // avoid returning negative lengths
		sr.pemappings = make([]format.PeImageMapping, sr.pe.MappingCount)
		err = cr.readBin(sr.pemappings, sr.stream.DataOffset, int64(binary.Size(&sr.pe)))
		if err != nil {
			return 0, 0, fmt.Errorf("reading PE image mappings: %s", err)
		}
		sr.peinit = true
	}
	d := int64(0)
	end := sr.pe.DataLength
	for _, m := range sr.pemappings {
		if int64(m.FileOffset) > off {
			end = int64(m.FileOffset)
			break
		}
		d = int64(m.Delta)
	}
	return d, end - off, nil
}

func (cr *Reader) readStream(sr *streamReader, b []byte) (_ int, err error) {
	n := len(b)
	rem := sr.stream.Size() - sr.off
	if int64(n) > rem {
		n = int(rem)
	}
	ro := sr.stream.DataOffset
	off := sr.off
	if sr.stream.Type() == format.StreamTypePeImage {
		delta, segrem, err := cr.getPESegment(sr, sr.off)
		if err != nil {
			return 0, err
		}
		if int64(n) > segrem {
			n = int(segrem)
		}
		ro = sr.pe.DataOffset
		off += delta
	}
	n, err = cr.readOffsetFull(b[:n], ro, off)
	sr.off += int64(n)
	rem -= int64(n)
	if err == nil && rem == 0 {
		err = io.EOF
	}
	return n, err
}

func (f *File) Read(b []byte) (_ int, err error) {
	defer func() {
		if err != nil && err != io.EOF {
			err = &ReaderError{Cim: f.r.name, Path: f.Name(), Op: "read", Err: err}
		}
	}()
	if f.IsDir() {
		return 0, ErrIsADirectory
	}
	return f.r.readStream(&f.sr, b)
}

const (
	ltNameOffSize = 4
	ltNameLenSize = 2
	ltSizeOff     = 0
	ltCountOff    = 4
	ltEntryOff    = 8
	fileIDSize    = 4
	streamSize    = 16
)

func parseName(b []byte, nos []byte, i int) ([]byte, error) {
	size := uint32(len(b))
	no := binary.LittleEndian.Uint32(nos[i*ltNameOffSize:])
	if no > size-ltNameLenSize {
		return nil, fmt.Errorf("invalid name offset %d > %d", no, size-ltNameLenSize)
	}
	nl := binary.LittleEndian.Uint16(b[no:])
	if mnl := (size - ltNameLenSize - no) / 2; uint32(nl) > mnl {
		return nil, fmt.Errorf("invalid name length %d > %d", nl, mnl)
	}
	return b[no+ltNameLenSize : no+ltNameLenSize+uint32(nl)*2], nil
}

func bsearchLinkTable(b []byte, esize int, name string, upcase []uint16) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	n := binary.LittleEndian.Uint32(b[ltCountOff:])
	es := b[ltEntryOff:]
	nos := es[n*uint32(esize):]
	lo := 0
	hi := int(n)
	for hi > lo {
		i := lo + (hi-lo)/2
		name16, err := parseName(b, nos, i)
		if err != nil {
			return nil, err
		}
		cmp := cmpcaseUtf8Utf16LE(name, name16, upcase)
		if cmp < 0 {
			hi = i
		} else if cmp == 0 {
			return es[i*esize : (i+1)*esize], nil
		} else {
			lo = i + 1
		}
	}
	return nil, nil
}

func enumLinkTable(b []byte, esize int, f func(string, []byte) error) error {
	if len(b) == 0 {
		return nil
	}
	var lt format.LinkTable
	r := bytes.NewReader(b)
	readBin(r, &lt)
	es := b[ltEntryOff:]
	nos := es[lt.LinkCount*fileIDSize:]
	for i := 0; i < int(lt.LinkCount); i++ {
		name, err := parseName(b, nos, i)
		if err != nil {
			return err
		}
		if err := f(parseUtf16LE(name), es[i*esize:(i+1)*esize]); err != nil {
			return err
		}
	}
	return nil
}

func validateLinkTable(b []byte, esize int) error {
	if len(b) < ltEntryOff {
		return fmt.Errorf("invalid link table size %d", len(b))
	}
	size := binary.LittleEndian.Uint32(b[ltSizeOff:])
	n := binary.LittleEndian.Uint32(b[ltCountOff:])
	if size < ltEntryOff {
		return fmt.Errorf("invalid link table size %d", size)
	}
	if int64(size) > int64(len(b)) {
		return fmt.Errorf("link table size mismatch %d < %d", len(b), size)
	}
	b = b[:size]
	if maxn := size - ltEntryOff/(uint32(esize)+ltNameOffSize); maxn < n {
		return fmt.Errorf("link table count mismatch %d < %d", maxn, n)
	}
	return nil
}

func (cr *Reader) getDirectoryTable(ino *inode) ([]byte, error) {
	if !ino.IsDir() || ino.file.DefaultStream.Size() == 0 {
		return nil, nil
	}
	cr.cm.Lock()
	b := ino.linkTable
	cr.cm.Unlock()
	if b == nil {
		b = make([]byte, ino.file.DefaultStream.Size())
		_, err := cr.readOffsetFull(b, ino.file.DefaultStream.DataOffset, 0)
		if err != nil {
			return nil, fmt.Errorf("reading directory link table: %s", err)
		}
		err = validateLinkTable(b, fileIDSize)
		if err != nil {
			return nil, err
		}
		cr.cm.Lock()
		ino.linkTable = b
		cr.cm.Unlock()
	}
	return b, nil
}

func (cr *Reader) findChild(ino *inode, name string) (format.FileID, error) {
	table, err := cr.getDirectoryTable(ino)
	if err != nil {
		return 0, err
	}
	if table != nil {
		b, err := bsearchLinkTable(table, fileIDSize, name, cr.upcase)
		if err != nil {
			return 0, err
		}
		if b != nil {
			return format.FileID(binary.LittleEndian.Uint32(b)), nil
		}
	}
	return 0, ErrFileNotFound
}

// Name returns the file's name.
func (f *File) Name() string {
	return f.name
}

// Readdir returns a slice of file names that are children of the directory.
// Fails if `f` is not a directory.
func (f *File) Readdir() (_ []string, err error) {
	defer func() {
		if err != nil {
			err = &ReaderError{Cim: f.r.name, Path: f.name, Op: "readdir", Err: err}
		}
	}()
	if !f.ino.IsDir() {
		return nil, ErrNotADirectory
	}
	table, err := f.r.getDirectoryTable(f.ino)
	if err != nil {
		return nil, err
	}
	var names []string
	err = enumLinkTable(table, fileIDSize, func(name string, fid []byte) error {
		names = append(names, name)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

func (cr *Reader) getStreamTable(ino *inode) ([]byte, error) {
	if ino.file.StreamTableOffset == format.NullOffset {
		return nil, nil
	}
	cr.cm.Lock()
	table := ino.streamTable
	cr.cm.Unlock()
	if table == nil {
		b, err := cr.readCounted(ino.file.StreamTableOffset, 4)
		if err != nil {
			return nil, fmt.Errorf("reading stream link table: %s", err)
		}
		err = validateLinkTable(b, streamSize)
		if err != nil {
			return nil, err
		}
		table = b
		cr.cm.Lock()
		ino.streamTable = table
		cr.cm.Unlock()
	}
	return table, nil
}

// Readstreams returns the names of the alternate data streams for a file.
func (f *File) Readstreams() (_ []string, err error) {
	defer func() {
		if err != nil {
			err = &ReaderError{Cim: f.r.name, Path: f.name, Op: "readstreams", Err: err}
		}
	}()
	table, err := f.r.getStreamTable(f.ino)
	if err != nil {
		return nil, err
	}
	var names []string
	err = enumLinkTable(table, streamSize, func(name string, stream []byte) error {
		names = append(names, name)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

// OpenStream opens an alternate data stream by name.
func (f *File) OpenStream(name string) (_ *Stream, err error) {
	defer func() {
		if err != nil {
			err = &ReaderError{Cim: f.r.name, Path: f.name, Stream: name, Op: "openstream", Err: err}
		}
	}()
	table, err := f.r.getStreamTable(f.ino)
	if err != nil {
		return nil, err
	}
	if table != nil {
		sb, err := bsearchLinkTable(table, streamSize, name, f.r.upcase)
		if err != nil {
			return nil, err
		}
		if sb != nil {
			s := &Stream{c: f.r, fname: f.name, name: name}
			readBin(bytes.NewReader(sb), &s.r.stream)
			if typ := s.r.stream.Type(); typ != format.StreamTypeData {
				return nil, fmt.Errorf("unsupported stream type %d", typ)
			}
			return s, nil
		}
	}
	return nil, ErrFileNotFound
}

func (s *Stream) Read(b []byte) (int, error) {
	n, err := s.c.readStream(&s.r, b)
	if err != nil && err != io.EOF {
		err = &ReaderError{Cim: s.c.name, Path: s.fname, Stream: s.name, Op: "read", Err: err}
	}
	return n, err
}

// A StreamInfo describes a stream.
type StreamInfo struct {
	Size int64
}

// Stat returns information about the stream.
func (s *Stream) Stat() (*StreamInfo, error) {
	return &StreamInfo{
		Size: s.r.stream.Size(),
	}, nil
}

// Name returns the name of the stream.
func (s *Stream) Name() string {
	return s.name
}