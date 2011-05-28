
-record(vfs_attr, {
	dir,     % bool(): true if node is directory
	size=0,  % int(): size in bytes
	mtime=0, % int(): seconds since epoch
	atime=0, % int(): seconds since epoch
	ctime=0  % int(): seconds since epoch
}).

-record(vfs_entry, {
	ino,      % int()
	attr,     % #vfs_attr{}
	attr_tmo, % int(): valid timeout for attributes
	entry_tmo % int(): timeout of entry name in ms
}).

-record(vfs_direntry, {
	name, % binary()
	attr  % #vfs_attr{}
}).

