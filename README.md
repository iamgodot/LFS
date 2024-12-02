# Simple Log-structured File System Simulator

## Description

The simulator imitates how a LFS works by always appending file changes to a log tail.

There's a checkpoint region to store locations of inode map pieces, while a inode map piece contains locations of inodes.

4 typical file operations are supported: file/directory creation, file writes and deletion.

Garbage collection gets executed to clean up blocks once the disk usage exceeds the preset threshold.

## Implementation

The simulator is written in Python, and about some implementation details:

1. A list of dictionaries are used as blocks in a disk
2. As how LFS works, the checkpoint region always stays in the first block
3. A `inode_map` dict is used to track all the inodes as cached in the main memory
4. Inodes use pointers to link to data blocks that store actual content
5. For directory, its size represents the number of files in it; while for regular files, size may refer to an offset near the end of pointer section

## Usage

For now all the parameters are defined in the script.

About the simulator:

- `NUM_IMAP_PTRS_IN_CR`: Maximum number of inode map pieces in the checkpoint region, defaults to 8
- `NUM_INODES_PER_IMAP_CHUNK`: Maximum number of inodes in a inode map piece, defaults to 8
- `NUM_INODE_PTRS`: Maximum number of data blocks pointed by an inode, defaults to 4
- `NUM_INODES`: Maximum number of inodes, defaults to 64
- `NUM_BLOCKS`: Maximum number of blocks, defaults to 256
- `GC_THRESHOLD`: Disk threshold for garbage collection, defaults to 0.8

About tests, 4 operations are randomly generated in various probabilities:

- File creation: 0.3
- File write: 0.4
- Directory creation: 0.2
- File deletion: 0.1

## Benchmarking

Due to limited time, the benchmarking is mainly about garbage collection. Experimental results with default settings show that:

1. About 4.52 new blocks are used for every file operation
2. Garbage collection is able to save 71.8% of previouly used space, and it's only required after about 58 file operations

## Todo

Besides garbage collection, LFS's performance boost comes from write buffering. Simply put, a bunch of file operations can be managed and merged to be written to the disk periodically, this includes yet doesn't limit to:

- Merged creation for hierachical directories
- Merged write of multiple writes and overwrites
- Cancellation of both file creation and deletion

## Reference

- The LFS simulator from OSTEP
- [Log-structured File System](https://www.cs.yale.edu/homes/aspnes/pinewiki/LogStructuredFilesystem.html)
- [Log-Structured File Systems](https://ops-class.org/slides/2017-04-17-lfs/)
- [The Design and Implementation of a Log-Structured File System](https://afterhoursacademic.medium.com/the-design-and-implementation-of-a-log-structured-file-system-20e1509a9ce1)
