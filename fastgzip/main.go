package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	gzip "github.com/klauspost/pgzip"
	"golang.org/x/sync/errgroup"
)

var (
	inFile      = flag.String("f", "", "input file. Required")
	directory   = flag.String("d", "", "output directory. Default is ./")
	splitNumber = flag.Int("n", 1, "split number. Saved as '{name}.{number}.gz' if 1 < n")
)

func exitWithHelp() {
	_, _ = fmt.Fprintf(os.Stderr, "%s [-options] [<input file>]\n", path.Base(os.Args[0]))
	flag.Usage()
	os.Exit(1)
}

func parse() {
	flag.Parse()
	args := []*string{inFile}
	if flag.NArg() > len(args) {
		exitWithHelp()
	}
	for i, value := range flag.Args() {
		*args[i] = value
	}
	if inFile == nil || *inFile == "" {
		_, _ = fmt.Fprintf(os.Stderr, "Error: input file is required\n")
		exitWithHelp()
	}
	if splitNumber != nil && *splitNumber < 1 {
		_, _ = fmt.Fprintf(os.Stderr, "Error: split number must be greater than 0\n")
		exitWithHelp()
	}
}

func run() error {
	parse()
	in, dir, n := *inFile, *directory, *splitNumber
	outFileNames := make([]string, n)
	if n == 1 {
		outFileNames[0] = path.Join(dir, in) + ".gz"
	} else {
		for i := 0; i < n; i++ {
			outFileNames[i] = fmt.Sprintf("%s.%d.gz", path.Join(dir, in), i+1)
		}
	}
	if dir != "" {
		_ = os.MkdirAll(dir, os.ModePerm) // ignore exist dir error
	}
	f, err := os.Open(in)
	if err != nil {
		err = fmt.Errorf("open file `%s` failed: %w", in, err)
		return err
	}

	ctx := context.Background()
	if strings.HasSuffix(in, ".gz") {
		return decompress(ctx, f)
	}
	return compress(ctx, f, outFileNames)
}

func compress(ctx context.Context, input io.Reader, outFileNames []string) error {
	g, ctx := errgroup.WithContext(ctx)
	ch := make(chan []byte, len(outFileNames)*2)
	for _, filename := range outFileNames {
		w, err := os.Create(filename)
		if err != nil {
			err = fmt.Errorf("create file `%s` failed: %w", filename, err)
			return err
		}
		g.Go(func() (err error) {
			gzWriter := gzip.NewWriter(w)
			defer func() { // close gzip writer
				if err = gzWriter.Flush(); err != nil {
					err = fmt.Errorf("flush file `%s` failed: %w", filename, err)
					return
				}
				if err = gzWriter.Close(); err != nil {
					err = fmt.Errorf("close file `%s` failed: %w", filename, err)
					return
				}
			}()
			for {
				select {
				case data, ok := <-ch:
					if !ok {
						return nil
					}
					if _, err = gzWriter.Write(data); err != nil {
						return err
					}
				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	r := bufio.NewReader(input)
	var buf []byte
	until := true
	for until {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			buf = append(buf, line...)
			continue
		}
		if len(buf) > 0 && len(line) > 0 {
			buf = append(buf, line...)
		}
		if errors.Is(err, io.EOF) {
			until = false
		}

		// write to channel
		if len(buf) > 0 {
			ch <- buf
			buf = buf[:0] // clear buffer
			continue
		}
		ch <- line
		continue

	}
	close(ch)
	return g.Wait()
}

func decompress(_ context.Context, _ io.Reader) error {
	return errors.New("decompress is not supported yet")
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
