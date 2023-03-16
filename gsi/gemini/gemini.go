package gemini

import (
    "os"
    "fmt"
    "log"
    "math"

    mmapgo "github.com/edsrzf/mmap-go"
    "encoding/binary"
    "golang.org/x/exp/mmap"
)

// Hello returns a greeting for the named person.
func Hello(name string) string {
    // Return a greeting that embeds the name in a message.
    message := fmt.Sprintf("Hi, %v. Welcome!", name)
    return message
}

// Hello returns a greeting for the named person.
func Hello2(name string) string {
    // Return a greeting that embeds the name in a message.
    message := fmt.Sprintf("Hi, %v. Welcome!", name)
    return message
}

func Yo( dim int64, index int64, count int64, offset int64 ) int64 {
    return 1
}

func Read_uint32_array(f *mmap.ReaderAt, arr [][]uint32, dim int64, index int64, count int64, offset int64) (int64,error) {

    // Iterate rows
    for j := 0; j < int(count); j++ {

        // Read consecutive 4 byte array into uint32 array, up to dims
        for i := 0; i < len(arr[j]); i++ {

            // Declare 4-byte array
            bt :=[]byte{0,0,0,0}

            // Compute offset for next uint32
            r_offset := offset + ( int64(j) + index)*dim*4 + int64(i)*4

            _, err := f.ReadAt(bt, r_offset);
            if err != nil {
                log.Fatalf("error reading file at offset: %d, %v", r_offset, err)
            }

            arr[j][i] = binary.LittleEndian.Uint32(bt)
        }

    }

    return dim, nil
}

func Read_float32_array(f *mmap.ReaderAt, arr [][]float32, dim int64, index int64, count int64, offset int64) (int64,error) {

    // Iterate rows
    for j := 0; j < int(count); j++ {

        // Read consecutive 4 byte array into uint32 array, up to dims
        for i := 0; i < len(arr[j]); i++ {

            // Declare 4-byte array
            bt :=[]byte{0,0,0,0}

            // Compute offset for next uint32
            r_offset := offset + ( int64(j) + index)*dim*4 + int64(i)*4

            _, err := f.ReadAt(bt, r_offset);
            if err != nil {
                log.Fatalf("error reading file at offset: %d, %v", r_offset, err)
            }

            bits := binary.LittleEndian.Uint32(bt)
            arr[j][i] = math.Float32frombits(bits)

            //arr[j][i] = binary.LittleEndian.Float32(bt)
        }

    }

    return dim, nil
}

func Append_uint32_array(fname string, arr [][]uint32, dim int64, count int64) {

    preheader := []byte{0x93,0x4e,0x55,0x4d,0x50,0x59,0x01,0x00,0x76,0x00}
    fmt_header := "{'descr': '<i4', 'fortran_order': False, 'shape': (%d, %d), }"
    empty := []byte{0x20}
    fin := []byte{0x0a}

    // Check if file exists
    fexists := true
    _, err := os.Stat(fname)
    if os.IsNotExist(err) {
        fexists = false
    }
    //fmt.Println("exists =", fexists)

    // Get file descriptor
    var f *os.File = nil
    if fexists {
        // Open file
        f, err = os.OpenFile(fname, os.O_RDWR, 0755 )
        if err != nil {
            log.Fatalf("error openingfile: %v", err)
        }
    } else {
        // Create file
        f, err = os.Create(fname)
        if err != nil {
            log.Fatalf("error creating file: %v", err)
        }
        // Create header area
        err = f.Truncate(int64(128))
        if err != nil {
            log.Fatalf("error resizing file for header: %v",  err)
        }
    }
    defer f.Close()

    // Get file size
    fi, err := f.Stat()
    if err != nil {
        log.Fatalf("error get file stats: %v", err)
    }
    file_size := int64( fi.Size() )

    // Get row count
    data_size := file_size - 128
    row_count := data_size / (dim *4)
    new_row_count := row_count + count

    // Resize file
    new_size := file_size + dim*4*count
    err = f.Truncate(int64(new_size))
    if err != nil {
        log.Fatalf("error resizing file: %v",  err)
    }

    // Memory map the new file
    mem, err := mmapgo.Map(f, mmapgo.RDWR, 0 )
    if err != nil {
        log.Fatalf("error mmapgo.Map: %v",  err)
    }
    defer mem.Unmap()

    // Create the new header
    header := fmt.Sprintf( fmt_header, new_row_count, dim )

    // Write the numpy header info
    idx :=0
    for i := 0; i < len(preheader); i++ {
        mem[idx] = preheader[i]
        idx += 1
    }
    for i := 0; i < len(header); i++ {
        mem[idx] = header[i]
        idx += 1
    }
    for i := idx; i < 128; i++ {
        mem[idx] = empty[0]
        idx += 1
    }
    mem[127] = fin[0]

    // append the arrays
    idx = int(128 + data_size)
    for j := 0; j< int(count); j++ {
        for i := 0; i< len(arr[j]); i++ {
            bt :=[]byte{0,0,0,0}
            binary.LittleEndian.PutUint32(bt, arr[j][i])
            mem[idx] = bt[0]
            mem[idx+1] = bt[1]
            mem[idx+2] = bt[2]
            mem[idx+3] = bt[3]
            idx += 4
        }
    }

    mem.Flush()

}

func Append_float32_array(fname string, arr [][]float32, dim int64, count int64) {

    preheader := []byte{0x93,0x4e,0x55,0x4d,0x50,0x59,0x01,0x00,0x76,0x00}
    fmt_header := "{'descr': '<f4', 'fortran_order': False, 'shape': (%d, %d), }"
    empty := []byte{0x20}
    fin := []byte{0x0a}

    // Check if file exists
    fexists := true
    _, err := os.Stat(fname)
    if os.IsNotExist(err) {
        fexists = false
    }
    //fmt.Println("exists =", fexists)

    // Get file descriptor
    var f *os.File = nil
    if fexists {
        // Open file
        f, err = os.OpenFile(fname, os.O_RDWR, 0755 )
        if err != nil {
            log.Fatalf("error openingfile: %v", err)
        }
    } else {
        // Create file
        f, err = os.Create(fname)
        if err != nil {
            log.Fatalf("error creating file: %v", err)
        }
        // Create header area
        err = f.Truncate(int64(128))
        if err != nil {
            log.Fatalf("error resizing file for header: %v",  err)
        }
    }
    defer f.Close()

    // Get file size
    fi, err := f.Stat()
    if err != nil {
        log.Fatalf("error get file stats: %v", err)
    }
    prev_file_size := int64( fi.Size() )

    // Get row count
    data_size := prev_file_size - 128
    row_count := data_size / (dim *4)
    new_row_count := row_count + count

    // Resize file
    new_size := prev_file_size + dim*4*count
    //fmt.Printf("Truncate sizes %d %d\n", prev_file_size, new_size)
    err = f.Truncate(int64(new_size))
    if err != nil {
        log.Fatalf("error resizing file: %v",  err)
    }

    // Memory map the new file
    mem, err := mmapgo.Map(f, mmapgo.RDWR, 0 )
    if err != nil {
        log.Fatalf("error mmapgo.Map: %v",  err)
    }
    defer mem.Unmap()
    // Create the new header
    header := fmt.Sprintf( fmt_header, new_row_count, dim )

    // Write the numpy header info
    idx :=0
    for i := 0; i < len(preheader); i++ {
        mem[idx] = preheader[i]
        idx += 1
    }
    for i := 0; i < len(header); i++ {
        mem[idx] = header[i]
        idx += 1
    }
    for i := idx; i < 128; i++ {
        mem[idx] = empty[0]
        idx += 1
    }
    mem[127] = fin[0]

    // fast-forward memmap index to the insert point
    idx = int(prev_file_size)
    //fmt.Printf("insert idx %d\n", idx)

    // append the arrays
    for j := 0; j< int(count); j++ {
        for i := 0; i< len(arr[j]); i++ {
            bt :=[]byte{0,0,0,0}
            //if j==0 {
            //    fmt.Printf("%d,%d:%f ", j,i, arr[j][i])
            //}
            tmp_uint32 := math.Float32bits(arr[j][i])
            binary.LittleEndian.PutUint32(bt, tmp_uint32)
            mem[idx] = bt[0]
            mem[idx+1] = bt[1]
            mem[idx+2] = bt[2]
            mem[idx+3] = bt[3]
            idx += 4
        }
        //if (j==0) {
        //    fmt.Println("") 
        //}
    }

    mem.Flush()

}

