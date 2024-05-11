# SplitL
`splitl` is a command-line tool that distributes the contents of a specified newline delimited file across multiple output files.
Note that the order of lines in the output files is not guaranteed to match the order in the input file.

## Usage

```
splitl [-options] [<input file>]

Usage of splitl:
    -d string
        output directory. Default is ./
    -f string
        input file. Required
    -n int
        number of output files. Must be greater than 1
```

## Install

```shell
go install github.com/2minchul/scripts/splitl@latest
```
