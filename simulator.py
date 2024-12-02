import random
import copy


# fixed addr
ADDR_CHECKPOINT_BLOCK = 0

# These are not (yet) things you can change
NUM_IMAP_PTRS_IN_CR = 8
NUM_INODES_PER_IMAP_CHUNK = 8
NUM_INODE_PTRS = 4

NUM_INODES = NUM_IMAP_PTRS_IN_CR * NUM_INODES_PER_IMAP_CHUNK
NUM_BLOCKS = NUM_INODES * NUM_INODE_PTRS

GC_THRESHOLD = 0.8

# block types
BLOCK_TYPE_CHECKPOINT = "type_cp"
BLOCK_TYPE_DATA_DIRECTORY = "type_data_dir"
BLOCK_TYPE_DATA_BLOCK = "type_data"
BLOCK_TYPE_INODE = "type_inode"
BLOCK_TYPE_IMAP = "type_imap"

# inode types
INODE_DIRECTORY = "dir"
INODE_REGULAR = "reg"

# root inode is "well known" per Unix conventions
ROOT_INODE = 0

# policies
ALLOCATE_SEQUENTIAL = 1


#
# Heart of simulation is found here
#
class LFS:
    def __init__(
        self,
        use_disk_cr=False,
        no_force_checkpoints=False,
        inode_policy=ALLOCATE_SEQUENTIAL,
    ):
        # whether to read checkpoint region and imap pieces from disk (if True)
        # or instead just to use "in-memory" inode map instead
        self.use_disk_cr = use_disk_cr

        # to force an update of the checkpoint region after each write
        self.no_force_checkpoints = no_force_checkpoints

        # inode allocation policy
        self.inode_policy = inode_policy

        # dump assistance
        self.dump_last = 1

        # ALL blocks are in the "disk"
        self.disk = []

        # checkpoint region (first block)
        self.cr = [-1] * NUM_IMAP_PTRS_IN_CR
        self.cr[0] = 3
        assert len(self.cr) == NUM_IMAP_PTRS_IN_CR

        # create first checkpoint region
        self.log({"block_type": BLOCK_TYPE_CHECKPOINT, "entries": self.cr})
        assert len(self.disk) == 1

        # init root dir data
        self.log(self.make_new_dirblock(ROOT_INODE, ROOT_INODE))
        assert len(self.disk) == 2

        # root inode
        root_inode = self.make_inode(itype=INODE_DIRECTORY, size=1, refs=2)
        root_inode["pointers"][0] = 1
        root_inode_address = self.log(root_inode)
        assert len(self.disk) == 3

        # init in memory imap
        self.inode_map = {}
        for i in range(NUM_INODES):
            self.inode_map[i] = -1
        self.inode_map[ROOT_INODE] = root_inode_address

        # imap piece
        self.log(self.make_imap_chunk(ROOT_INODE))
        assert len(self.disk) == 4

        # error code: tracking
        self.error_clear()
        return

    def make_data_block(self, data):
        return {"block_type": BLOCK_TYPE_DATA_BLOCK, "contents": data}

    def make_inode(self, itype, size, refs):
        return {
            "block_type": BLOCK_TYPE_INODE,
            "type": itype,
            "size": size,
            "refs": refs,
            "pointers": [-1] * NUM_INODE_PTRS,
        }

    def make_new_dirblock(self, parent_inum, current_inum):
        dirblock = self.make_empty_dirblock()
        dirblock["entries"][0] = (".", current_inum)
        dirblock["entries"][1] = ("..", parent_inum)
        return dirblock

    def make_empty_dirblock(self):
        return {
            "block_type": BLOCK_TYPE_DATA_DIRECTORY,
            "entries": [
                ("-", -1),
                ("-", -1),
                ("-", -1),
                ("-", -1),
            ],
        }

    def make_imap_chunk(self, cnum):
        imap_chunk = {}
        imap_chunk["block_type"] = BLOCK_TYPE_IMAP
        imap_chunk["entries"] = list()
        start = cnum * NUM_INODES_PER_IMAP_CHUNK
        for i in range(start, start + NUM_INODES_PER_IMAP_CHUNK):
            imap_chunk["entries"].append(self.inode_map[i])
        return imap_chunk

    def make_random_blocks(self, num):
        contents = []
        for i in range(num):
            L = chr(ord("a") + int(random.random() * 26))
            contents.append(str(16 * ("%s%d" % (L, i))))
        return contents

    def inum_to_chunk(self, inum):
        return int(inum / NUM_INODES_PER_IMAP_CHUNK)

    def determine_liveness(self):
        # first, assume all are dead
        self.live = {}
        for i in range(len(self.disk)):
            self.live[i] = False

        # checkpoint region
        self.live[0] = True

        # now mark latest pieces of imap as live
        for ptr in self.cr:
            if ptr == -1:
                continue
            self.live[ptr] = True

        # go through imap, find live inodes and their addresses
        # latest inodes are all live, by def
        inodes = []
        for i in range(len(self.inode_map)):
            if self.inode_map[i] == -1:
                continue
            self.live[self.inode_map[i]] = True
            inodes.append(i)

        # go through live inodes and find blocks each points to
        for i in inodes:
            inode = self.disk[self.inode_map[i]]
            for ptr in inode["pointers"]:
                self.live[ptr] = True
        return

    def gc(self):
        self.determine_liveness()
        block_no_mappings = {}
        block_index_new = 0
        block_nos_old = []
        for i in range(len(self.disk)):
            if self.live[i]:
                block_nos_old.append(i)
        block_nos_old.sort()

        # map old block number to new index
        for i in block_nos_old:
            block_no_mappings[i] = block_index_new
            block_index_new += 1

        # update block numbers in inodes
        for i in block_nos_old:
            block = copy.deepcopy(self.disk[i])
            block_type = block["block_type"]
            if block_type == BLOCK_TYPE_INODE:
                pointers = block["pointers"]
                for j in range(len(pointers)):
                    value = pointers[j]
                    if value in block_no_mappings:
                        pointers[j] = block_no_mappings[value]
            elif block_type in [BLOCK_TYPE_IMAP, BLOCK_TYPE_CHECKPOINT]:
                entries = block["entries"]
                for j in range(len(entries)):
                    value = entries[j]
                    if value in block_no_mappings:
                        entries[j] = block_no_mappings[value]
            self.disk[block_no_mappings[i]] = block

        # remove cleaned blocks
        block_num_prev = len(self.disk)
        block_num_cur = len(block_no_mappings)
        self.disk = self.disk[:block_num_cur]
        print(
            f"Garbage collection finished, reduced {block_num_prev} blocks to {block_num_cur} now."
        )
        print(f"Saved {(block_num_prev - block_num_cur) / block_num_prev}% space.")

        # update inode map in memory
        for i in range(len(self.inode_map)):
            block_no = self.inode_map[i]
            if block_no != -1:
                self.inode_map[i] = block_no_mappings[block_no]

    def error_log(self, s):
        self.error_list.append(s)
        return

    def error_clear(self):
        self.error_list = []
        return

    def error_dump(self):
        for i in self.error_list:
            print("  %s" % i)
        return

    def dump_partial(self, show_liveness, show_checkpoint):
        if show_checkpoint or not self.no_force_checkpoints:
            self.__dump(0, 1)
        if not self.no_force_checkpoints:
            print("...")
        self.__dump(self.dump_last, len(self.disk))
        self.dump_last = len(self.disk)
        return

    def dump(self):
        self.__dump(0, len(self.disk))
        return

    def __dump(self, start, end):
        self.determine_liveness()

        for i in range(start, end):
            # print ADDRESS on disk
            b = self.disk[i]
            block_type = b["block_type"]
            print("[ %3d ]" % i, end="")

            # print LIVENESS
            if self.live[i]:
                print(" live", end=" ")
            else:
                print("     ", end=" ")

            if block_type == BLOCK_TYPE_CHECKPOINT:
                print("checkpoint:", end=" ")
                for e in b["entries"]:
                    if e != -1:
                        print(e, end=" ")
                    else:
                        print("--", end=" ")
                print("")
            elif block_type == BLOCK_TYPE_DATA_DIRECTORY:
                for e in b["entries"]:
                    if e[1] != -1:
                        print("[%s,%s]" % (str(e[0]), str(e[1])), end=" ")
                    else:
                        print("--", end=" ")
                print("")
            elif block_type == BLOCK_TYPE_DATA_BLOCK:
                print(b["contents"])
            elif block_type == BLOCK_TYPE_INODE:
                print(
                    "type:" + b["type"],
                    "size:" + str(b["size"]),
                    "refs:" + str(b["refs"]),
                    "ptrs:",
                    end=" ",
                )
                for p in b["pointers"]:
                    if p != -1:
                        print("%s" % p, end=" ")
                    else:
                        print("--", end=" ")
                print("")
            elif block_type == BLOCK_TYPE_IMAP:
                print("chunk(imap):", end=" ")
                for e in b["entries"]:
                    if e != -1:
                        print(e, end=" ")
                    else:
                        print("--", end=" ")
                print("")
            else:
                print("error: unknown block_type", block_type)
                exit(1)
        return

    def log(self, block):
        new_address = len(self.disk)
        self.disk.append(copy.deepcopy(block))
        return new_address

    def allocate_inode(self):
        for i in range(len(self.inode_map)):
            if self.inode_map[i] == -1:
                self.inode_map[i] = 1
                return i
        return -1

    def free_inode(self, inum):
        assert self.inode_map[inum] != -1
        self.inode_map[inum] = -1
        return

    def remap(self, inode_number, inode_address):
        self.inode_map[inode_number] = inode_address
        return

    def dump_inode_map(self):
        for i in range(len(self.inode_map)):
            if self.inode_map[i] != -1:
                print("  ", i, "->", self.inode_map[i])
        print("")
        return

    def cr_sync(self):
        # only place in code where an OVERWRITE occurs
        self.disk[ADDR_CHECKPOINT_BLOCK] = copy.deepcopy(
            {"block_type": BLOCK_TYPE_CHECKPOINT, "entries": self.cr}
        )
        return 0

    def get_inode_from_inumber(self, inode_number):
        imap_entry_index = int(inode_number / NUM_INODES_PER_IMAP_CHUNK)
        imap_entry_offset = inode_number % NUM_INODES_PER_IMAP_CHUNK

        if self.use_disk_cr:
            # this is the disk path
            checkpoint_block = self.disk[ADDR_CHECKPOINT_BLOCK]
            assert checkpoint_block["block_type"] == BLOCK_TYPE_CHECKPOINT

            imap_block_address = checkpoint_block["entries"][imap_entry_index]
            imap_block = self.disk[imap_block_address]
            assert imap_block["block_type"] == BLOCK_TYPE_IMAP

            inode_address = imap_block["entries"][imap_entry_offset]
        else:
            # this is the just-use-the-mem-inode_map path
            inode_address = self.inode_map[inode_number]

        assert inode_address != -1
        inode = self.disk[inode_address]
        assert inode["block_type"] == BLOCK_TYPE_INODE
        return inode

    def __lookup(self, parent_inode_number, name):
        parent_inode = self.get_inode_from_inumber(parent_inode_number)
        assert parent_inode["type"] == INODE_DIRECTORY
        for address in parent_inode["pointers"]:
            if address == -1:
                continue
            directory_block = self.disk[address]
            assert directory_block["block_type"] == BLOCK_TYPE_DATA_DIRECTORY
            for entry_name, entry_inode_number in directory_block["entries"]:
                if entry_name == name:
                    return (entry_inode_number, parent_inode)
        return (-1, parent_inode)

    def __walk_path(self, path):
        split_path = path.split("/")
        if split_path[0] != "":
            self.error_log("path malformed: must start with /")
            return -1, "", -1, ""
        inode_number = -1
        parent_inode_number = ROOT_INODE  # root inode number is well known
        for i in range(1, len(split_path) - 1):
            inode_number, inode = self.__lookup(parent_inode_number, split_path[i])
            if inode_number == -1:
                self.error_log("directory %s not found" % split_path[i])
                return -1, "", -1, ""
            if inode["type"] != INODE_DIRECTORY:
                self.error_log(
                    "invalid element of path [%s] (not a dir)" % split_path[i]
                )
                return -1, "", -1, ""
            parent_inode_number = inode_number

        file_name = split_path[len(split_path) - 1]
        inode_number, parent_inode = self.__lookup(parent_inode_number, file_name)
        return inode_number, file_name, parent_inode_number, parent_inode

    def update_imap(self, inum_list):
        chunk_list = list()
        for inum in inum_list:
            cnum = self.inum_to_chunk(inum)
            if cnum not in chunk_list:
                chunk_list.append(cnum)
                self.log(self.make_imap_chunk(cnum))
                self.cr[cnum] = len(self.disk) - 1
        return

    def __read_dirblock(self, inode, index):
        return self.disk[inode["pointers"][index]]

    # return (inode_index, dirblock_index)
    def __find_matching_dir_slot(self, name, inode):
        for inode_index in range(inode["size"]):
            directory_block = self.__read_dirblock(inode, inode_index)
            assert directory_block["block_type"] == BLOCK_TYPE_DATA_DIRECTORY

            for slot_index in range(len(directory_block["entries"])):
                entry_name, entry_inode_number = directory_block["entries"][slot_index]
                if entry_name == name:
                    return inode_index, slot_index
        return -1, -1

    def __add_dir_entry(self, parent_inode, file_name, inode_number):
        # this will be the directory block to contain the new name->inum mapping
        inode_index, dirblock_index = self.__find_matching_dir_slot("-", parent_inode)

        if inode_index != -1:
            # there is room in existing block: make copy, update it, and log it
            index_to_update = inode_index
            parent_size = parent_inode["size"]

            new_directory_block = copy.deepcopy(
                self.__read_dirblock(parent_inode, inode_index)
            )
            new_directory_block["entries"][dirblock_index] = (file_name, inode_number)
        else:
            # no room in existing directory block: allocate new one IF there is room in inode to point to it
            if parent_inode["size"] != NUM_INODE_PTRS:
                index_to_update = parent_inode["size"]
                parent_size = index_to_update + 1

                new_directory_block = self.make_empty_dirblock()
                new_directory_block["entries"][0] = (file_name, inode_number)
            else:
                return -1, -1, {}
        return index_to_update, parent_size, new_directory_block

    # create (file OR dir)
    def __file_create(self, path, is_file):
        inode_number, file_name, parent_inode_number, parent_inode = self.__walk_path(
            path
        )
        if inode_number != -1:
            # self.error_log('create failed: file %s already exists' % path)
            self.error_log("create failed: file already exists")
            return -1

        if parent_inode_number == -1:
            self.error_log("create failed: walkpath returned error [%s]" % path)
            return -1

        # finally, allocate inode number for new file/dir
        new_inode_number = self.allocate_inode()
        if new_inode_number == -1:
            self.error_log("create failed: no more inodes available")
            return -1

        # this will be the directory block to contain the new name->inum mapping
        index_to_update, parent_size, new_directory_block = self.__add_dir_entry(
            parent_inode, file_name, new_inode_number
        )
        if index_to_update == -1:
            self.error_log("error: directory is full (path %s)" % path)
            self.free_inode(new_inode_number)
            return -1

        # log directory data block (either new version of old OR new one entirely)
        new_directory_block_address = self.log(new_directory_block)

        # now have to make new version of directory inode
        # update size (if needed), inc refs if this is a dir, point to new dir block addr
        new_parent_inode = copy.deepcopy(parent_inode)
        new_parent_inode["size"] = parent_size
        if not is_file:
            new_parent_inode["refs"] += 1
        new_parent_inode["pointers"][index_to_update] = new_directory_block_address

        # if directory, must create empty dir block
        if not is_file:
            self.log(self.make_new_dirblock(parent_inode_number, new_inode_number))
            new_dirblock_address = len(self.disk) - 1

        # and the new inode itself
        if is_file:
            # create empty file by default
            new_inode = self.make_inode(itype=INODE_REGULAR, size=0, refs=1)
        else:
            # create directory inode and point it to the one dirblock it owns
            new_inode = self.make_inode(itype=INODE_DIRECTORY, size=1, refs=2)
            new_inode["pointers"][0] = new_dirblock_address

        #
        # ADD updated parent inode, file/dir inode TO LOG
        #
        new_parent_inode_address = self.log(new_parent_inode)
        new_inode_address = self.log(new_inode)

        # and new imap entries for both parent and new inode
        self.remap(parent_inode_number, new_parent_inode_address)
        self.remap(new_inode_number, new_inode_address)

        # finally, create new chunk of imap
        self.update_imap([parent_inode_number, new_inode_number])

        # SYNC checkpoint region
        if not self.no_force_checkpoints:
            self.cr_sync()
        return 0

    def _space_check(self):
        if len(self.disk) > NUM_BLOCKS * GC_THRESHOLD:
            print(f"Used {len(self.disk)} blocks now, triggering garbage collection...")
            self.gc()

    # file_create()
    def file_create(self, path):
        self.error_clear()
        self._space_check()
        return self.__file_create(path, True)

    # dir_create()
    def dir_create(self, path):
        self.error_clear()
        self._space_check()
        return self.__file_create(path, False)

    def file_write(self, path, offset, num_blks):
        self.error_clear()
        self._space_check()

        # just make up contents of data blocks - up to the max spec'd by write
        # note: may not write all of these, because of running out of room in inode...
        contents = self.make_random_blocks(num_blks)

        inode_number, file_name, parent_inode_number, parent_inode = self.__walk_path(
            path
        )
        if inode_number == -1:
            self.error_log("write failed: file not found [path %s]" % path)
            return -1

        inode = self.get_inode_from_inumber(inode_number)
        if inode["type"] != INODE_REGULAR:
            self.error_log("write failed: cannot write to non-regular file %s" % path)
            return -1

        if offset < 0 or offset >= NUM_INODE_PTRS:
            self.error_log("write failed: bad offset %d" % offset)
            return -1

        # create potential write list -- up to max file size
        current_log_ptr = len(self.disk)
        current_offset = offset
        potential_writes = []
        while current_offset < NUM_INODE_PTRS and current_offset < offset + len(
            contents
        ):
            potential_writes.append((current_offset, current_log_ptr))
            current_offset += 1
            current_log_ptr += 1

        # write data block(s)
        for i in range(len(potential_writes)):
            self.log(self.make_data_block(contents[i]))

        # write new version of inode, with updated size
        new_inode = copy.deepcopy(inode)
        new_inode["size"] = max(current_offset, inode["size"])
        for new_offset, new_addr in potential_writes:
            new_inode["pointers"][new_offset] = new_addr
        new_inode_address = self.log(new_inode)

        # write new chunk of imap
        self.remap(inode_number, new_inode_address)
        self.log(self.make_imap_chunk(self.inum_to_chunk(inode_number)))
        self.cr[self.inum_to_chunk(inode_number)] = len(self.disk) - 1

        # write checkpoint region
        if not self.no_force_checkpoints:
            self.cr_sync()

        # return size of write (total # written, not desired, may be less than asked for)
        return current_offset - offset

    def file_delete(self, path):
        self.error_clear()
        self._space_check()

        inode_number, file_name, parent_inode_number, parent_inode = self.__walk_path(
            path
        )
        if inode_number == -1:
            self.error_log("delete failed: file not found [%s]" % path)
            return -1

        inode = self.get_inode_from_inumber(inode_number)
        if inode["type"] != INODE_REGULAR:
            self.error_log("delete failed: cannot delete non-regular file [%s]" % path)
            return -1

        # have to check: is the file actually down to its last ref?
        if inode["refs"] == 1:
            self.free_inode(inode_number)

        # now, find entry in DIRECTORY DATA BLOCK and zero it
        inode_index, dirblock_index = self.__find_matching_dir_slot(
            file_name, parent_inode
        )
        assert inode_index != -1
        new_directory_block = copy.deepcopy(
            self.__read_dirblock(parent_inode, inode_index)
        )
        new_directory_block["entries"][dirblock_index] = ("-", -1)

        # this leads to DIRECTORY DATA, DIR INODE, (and hence IMAP_CHUNK, CR_SYNC) writes
        dir_addr = self.log(new_directory_block)

        new_parent_inode = copy.deepcopy(parent_inode)
        new_parent_inode["pointers"][inode_index] = dir_addr
        new_parent_inode_addr = self.log(new_parent_inode)
        self.remap(parent_inode_number, new_parent_inode_addr)

        # if this ISNT the last link, decrease ref count and output new version
        if inode["refs"] > 1:
            new_inode = copy.deepcopy(inode)
            new_inode["refs"] -= 1
            new_inode_addr = self.log(new_inode)
            self.remap(inode_number, new_inode_addr)

        # create new chunk of imap
        self.update_imap([inode_number, parent_inode_number])

        # and sync if need be
        if not self.no_force_checkpoints:
            self.cr_sync()
        return 0


def pick_random(a_list):
    if len(a_list) == 0:
        return ""
    index = int(random.random() * len(a_list))
    return a_list[index]


def make_random_file_name(parent_dir):
    L1 = chr(ord("a") + int(random.random() * 26))
    L2 = chr(ord("a") + int(random.random() * 26))
    N1 = str(int(random.random() * 10))
    if parent_dir == "/":
        return "/" + L1 + L2 + N1
    return parent_dir + "/" + L1 + L2 + N1


def make_commands(num_commands, percents):
    commands = []
    existing_files = []
    existing_dirs = ["/"]
    while num_commands > 0:
        chances = random.random()
        command = ""
        if chances >= percents["c"][0] and chances < percents["c"][1]:
            pdir = pick_random(existing_dirs)
            if pdir == "":
                continue
            nfile = make_random_file_name(pdir)
            command = "c,%s" % nfile
            existing_files.append(nfile)
        elif chances >= percents["w"][0] and chances < percents["w"][1]:
            pfile = pick_random(existing_files)
            if pfile == "":
                continue
            woff = int(random.random() * 8)
            wlen = int(random.random() * 8)
            command = "w,%s,%d,%d" % (pfile, woff, wlen)
        elif chances >= percents["d"][0] and chances < percents["d"][1]:
            pdir = pick_random(existing_dirs)
            if pdir == "":
                continue
            ndir = make_random_file_name(pdir)
            command = "d,%s" % ndir
            existing_dirs.append(ndir)
        elif chances >= percents["r"][0] and chances < percents["r"][1]:
            if len(existing_files) == 0:
                continue
            index = int(random.random() * len(existing_files))
            command = "r,%s" % existing_files[index]
            del existing_files[index]
        else:
            print("abort: internal error with percent operations")
            exit(1)
        commands.append(command)
        num_commands -= 1
    return commands


def parse_and_execute(commands):
    L = LFS()
    print()
    print("INITIAL file system contents:")
    L.dump()
    L.dump_last = 4
    print()

    disk_len = len(L.disk)
    block_usage = []
    for i in range(len(commands)):
        command_and_args = commands[i].split(",")
        if command_and_args[0] == "c":
            print("create file", command_and_args[1])
            L.file_create(command_and_args[1])
        elif command_and_args[0] == "d":
            print("create dir ", command_and_args[1])
            L.dir_create(command_and_args[1])
        elif command_and_args[0] == "r":
            print("delete file", command_and_args[1])
            L.file_delete(command_and_args[1])
        elif command_and_args[0] == "w":
            print(
                "write file  %s offset=%d size=%d"
                % (
                    command_and_args[1],
                    int(command_and_args[2]),
                    int(command_and_args[3]),
                ),
            )
            L.file_write(
                command_and_args[1], int(command_and_args[2]), int(command_and_args[3])
            )
        else:
            print("command not understood so skipping [%s]" % command_and_args[0])

        L.dump_partial(False, False)
        print()
        block_usage.append(len(L.disk) - disk_len)
        disk_len = len(L.disk)

    print("FINAL file system contents:")
    L.dump()
    block_usage = [i for i in block_usage if i >= 0]
    print(
        "Average block addition per file operation:", sum(block_usage) / len(commands)
    )
    # L.gc()
    # print("After GC:")
    # L.dump()
    # print(f"Disk usage reduced from {disk_len} to {len(L.disk)}")


def benchmark():
    percents = {"c": (0.0, 0.3), "w": (0.3, 0.7), "d": (0.7, 0.9), "r": (0.9, 1.0)}
    commands = make_commands(60, percents)
    parse_and_execute(commands)


if __name__ == "__main__":
    benchmark()
