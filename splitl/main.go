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

	"golang.org/x/sync/errgroup"
)

var (
	inFile      = flag.String("f", "", "input file. Required")
	directory   = flag.String("d", "", "output directory. Default is ./")
	splitNumber = flag.Int("n", 0, "number of output files. Must be greater than 1")
)

func exitWithHelp() {
	fmt.Fprintf(os.Stderr, "%s [-options] [<input file>]\n", path.Base(os.Args[0]))
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
	if *inFile == "" {
		fmt.Fprintf(os.Stderr, "Error: input file is required\n")
		exitWithHelp()
	}
	if *splitNumber < 2 {
		fmt.Fprintf(os.Stderr, "Error: split number must be greater than 1\n")
		exitWithHelp()
	}
}

func run() error {
	parse()

	in, dir, n := *inFile, *directory, *splitNumber

	outFileNames := make([]string, n)
	ext := path.Ext(in)
	base := strings.TrimSuffix(path.Base(in), ext)
	for i := 0; i < n; i++ {
		outFileNames[i] = fmt.Sprintf("%s.%d%s", path.Join(dir, base), i+1, ext)
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
	return splitFiles(ctx, f, outFileNames)
}

func splitFiles(ctx context.Context, input io.Reader, outFileNames []string) error {
	g, ctx := errgroup.WithContext(ctx)
	ch := make(chan []byte, len(outFileNames)*2)
	for _, filename := range outFileNames {
		f, err := os.Create(filename)
		if err != nil {
			err = fmt.Errorf("create file `%s` failed: %w", filename, err)
			return err
		}
		g.Go(func() (err error) {
			defer func() {
				err = f.Close()
				if err != nil {
					err = fmt.Errorf("close file `%s` failed: %w", filename, err)
					return
				}
			}()
			w := bufio.NewWriter(f)
			defer func() {
				err = w.Flush()
				if err != nil {
					err = fmt.Errorf("flush file `%s` failed: %w", filename, err)
					return
				}
			}()

			for {
				select {
				case data, ok := <-ch:
					if !ok {
						return nil
					}
					if _, err = w.Write(data); err != nil {
						return fmt.Errorf("write to file `%s` failed: %w", filename, err)
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
	}
	close(ch)
	return g.Wait()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
