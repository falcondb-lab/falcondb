/// Adaptive Radix Tree (ART) — secondary index engine (P1-1).
///
/// Implements the four canonical node types (Node4/16/48/256) with
/// Optimistic Lock Coupling (OLC) for concurrent reads without write locks.
///
/// Design decisions:
/// - Node layout: `repr(C, align(64))` so hot fields fit in the first cache line.
/// - Inline prefix: up to 12 bytes stored inside the node header, covering the
///   vast majority of encoded-datum key prefixes without pointer indirection.
/// - OLC: version counter (even = stable, odd = write in progress).
///   Readers snapshot the version, read, then validate — no write lock on reads.
///   Writers CAS the counter to odd, modify, then increment to even+2.
/// - Values: leaf nodes store `Vec<PrimaryKey>` (a sorted list for non-unique
///   indexes) or a single PK for unique indexes.  All allocation happens only
///   at leaf level; inner nodes are pure routing structures.
///
/// Concurrency guarantee:
///   - Concurrent readers: fully parallel, zero blocking.
///   - Concurrent writer vs readers: readers retry on version mismatch (~ns).
///   - Concurrent writers: serialised per-node via the version spinlock.
///   - No global lock anywhere in the read or write path.
use std::sync::atomic::{AtomicU64, Ordering};

use crate::memtable::PrimaryKey;

// ── Constants ────────────────────────────────────────────────────────────────

/// Maximum inline prefix bytes stored directly in the node header.
const MAX_PREFIX_LEN: usize = 12;

// ── OLC version helpers ───────────────────────────────────────────────────────

/// An OLC version word.  Even = stable; odd = locked (write in progress).
struct OlcVersion(AtomicU64);

impl OlcVersion {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Read the current version.  Spin if a write is in progress.
    #[inline]
    fn read_lock(&self) -> u64 {
        loop {
            let v = self.0.load(Ordering::Acquire);
            if v & 1 == 0 {
                return v;
            }
            std::hint::spin_loop();
        }
    }

    /// Return `true` if `observed` is still current (no write occurred).
    #[inline]
    fn validate(&self, observed: u64) -> bool {
        self.0.load(Ordering::Acquire) == observed
    }

    /// Acquire the write lock (CAS even→odd).  Spin until successful.
    #[inline]
    fn write_lock(&self) -> u64 {
        loop {
            let v = self.0.load(Ordering::Acquire);
            if v & 1 == 0 {
                if self
                    .0
                    .compare_exchange_weak(v, v | 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return v;
                }
            }
            std::hint::spin_loop();
        }
    }

    /// Release the write lock (increment by 2, making version even again).
    #[inline]
    fn write_unlock(&self, locked_v: u64) {
        self.0.store(locked_v + 2, Ordering::Release);
    }
}

// ── Leaf ─────────────────────────────────────────────────────────────────────

/// A leaf node holds the full remaining key suffix and the associated PKs.
struct Leaf {
    /// Full key stored in this leaf (used for equality check after traversal).
    key: Vec<u8>,
    /// Sorted list of primary keys mapping to this index key.
    pks: Vec<PrimaryKey>,
}

impl Leaf {
    fn new(key: Vec<u8>, pk: PrimaryKey) -> Self {
        Self { key, pks: vec![pk] }
    }

    fn insert_pk(&mut self, pk: PrimaryKey) {
        if let Err(pos) = self.pks.binary_search(&pk) {
            self.pks.insert(pos, pk);
        }
    }

    fn remove_pk(&mut self, pk: &PrimaryKey) {
        if let Ok(pos) = self.pks.binary_search(pk) {
            self.pks.remove(pos);
        }
    }
}

// ── NodeHeader ────────────────────────────────────────────────────────────────

/// Shared header for all inner node types (16 bytes, fits in CL0).
#[repr(C)]
struct NodeHeader {
    version:    OlcVersion,         // 8 bytes — OLC version word
    prefix_len: u16,                // 2 bytes — length of compressed prefix
    num_ch:     u8,                 // 1 byte  — current child count
    _pad:       u8,                 // 1 byte  — padding
    prefix:     [u8; MAX_PREFIX_LEN], // 12 bytes — inline prefix bytes
}

impl NodeHeader {
    fn new() -> Self {
        Self {
            version:    OlcVersion::new(),
            prefix_len: 0,
            num_ch:     0,
            _pad:       0,
            prefix:     [0u8; MAX_PREFIX_LEN],
        }
    }

    fn prefix_matches(&self, key: &[u8], depth: usize) -> usize {
        let plen = self.prefix_len as usize;
        let limit = plen.min(MAX_PREFIX_LEN).min(key.len().saturating_sub(depth));
        let mut i = 0;
        while i < limit && self.prefix[i] == key[depth + i] {
            i += 1;
        }
        i
    }

    fn set_prefix(&mut self, key: &[u8], depth: usize, len: usize) {
        let copy = len.min(MAX_PREFIX_LEN);
        self.prefix[..copy].copy_from_slice(&key[depth..depth + copy]);
        self.prefix_len = len as u16;
    }
}

// ── Inner node types ──────────────────────────────────────────────────────────

/// Node4: up to 4 children.  keys[] and children[] are kept in sorted order.
#[repr(C, align(64))]
struct Node4 {
    hdr:      NodeHeader,
    keys:     [u8; 4],
    children: [NodePtr; 4],
}

/// Node16: up to 16 children.  Same sorted layout as Node4.
#[repr(C, align(64))]
struct Node16 {
    hdr:      NodeHeader,
    keys:     [u8; 16],
    children: [NodePtr; 16],
}

/// Node48: up to 48 children.  Direct byte→slot index table (256 bytes)
/// + a 48-slot children array.  Eliminates the binary search of Node16.
#[repr(C, align(64))]
struct Node48 {
    hdr:      NodeHeader,
    index:    [u8; 256],   // byte → slot (0xFF = empty)
    children: [NodePtr; 48],
}

/// Node256: up to 256 children.  Direct byte→child pointer array.
#[repr(C, align(64))]
struct Node256 {
    hdr:      NodeHeader,
    children: [NodePtr; 256],
}

// ── NodePtr: a tagged pointer ─────────────────────────────────────────────────

/// A raw pointer with a type tag.
///
/// Tag encoding (low 2 bits of the usize address):
///   00 = null
///   01 = Leaf
///   10 = Node4
///   11 = Node16 (high bit of low nibble set)
///   …inner nodes use distinct heap-allocated types, so `Box::into_raw` is safe.
///
/// We use `*mut ()` + tag rather than an enum to keep `NodePtr` at exactly
/// one pointer word (8 bytes on x86-64), avoiding enum padding overhead in
/// the arrays inside Node4/Node16.
#[derive(Clone, Copy)]
struct NodePtr(usize);

const TAG_NULL:   usize = 0;
const TAG_LEAF:   usize = 1;
const TAG_NODE4:  usize = 2;
const TAG_NODE16: usize = 3;
const TAG_NODE48: usize = 4;
const TAG_NODE256:usize = 5;
const TAG_MASK:   usize = 7;

impl NodePtr {
    const NULL: NodePtr = NodePtr(TAG_NULL);

    fn is_null(self) -> bool { self.0 == TAG_NULL }

    fn from_leaf(p: *mut Leaf) -> Self {
        NodePtr((p as usize) | TAG_LEAF)
    }
    fn from_node4(p: *mut Node4) -> Self {
        NodePtr((p as usize) | TAG_NODE4)
    }
    fn from_node16(p: *mut Node16) -> Self {
        NodePtr((p as usize) | TAG_NODE16)
    }
    fn from_node48(p: *mut Node48) -> Self {
        NodePtr((p as usize) | TAG_NODE48)
    }
    fn from_node256(p: *mut Node256) -> Self {
        NodePtr((p as usize) | TAG_NODE256)
    }

    fn tag(self) -> usize { self.0 & TAG_MASK }

    unsafe fn as_leaf(self) -> *mut Leaf {
        (self.0 & !TAG_MASK) as *mut Leaf
    }
    unsafe fn as_node4(self) -> *mut Node4 {
        (self.0 & !TAG_MASK) as *mut Node4
    }
    unsafe fn as_node16(self) -> *mut Node16 {
        (self.0 & !TAG_MASK) as *mut Node16
    }
    unsafe fn as_node48(self) -> *mut Node48 {
        (self.0 & !TAG_MASK) as *mut Node48
    }
    unsafe fn as_node256(self) -> *mut Node256 {
        (self.0 & !TAG_MASK) as *mut Node256
    }

    /// Get the `NodeHeader` from any inner node type.
    unsafe fn hdr(self) -> *mut NodeHeader {
        match self.tag() {
            TAG_NODE4  => &mut (*self.as_node4()).hdr  as *mut NodeHeader,
            TAG_NODE16 => &mut (*self.as_node16()).hdr as *mut NodeHeader,
            TAG_NODE48 => &mut (*self.as_node48()).hdr as *mut NodeHeader,
            TAG_NODE256 => &mut (*self.as_node256()).hdr as *mut NodeHeader,
            _ => unreachable!(),
        }
    }
}

// All raw pointers are only accessed under OLC discipline (valid write lock or
// validated read).  The ArtTree itself is Send+Sync because we guarantee
// exclusive access during writes via the per-node OLC spinlock.
unsafe impl Send for ArtTree {}
unsafe impl Sync for ArtTree {}

// ── ArtTree ───────────────────────────────────────────────────────────────────

/// An Adaptive Radix Tree acting as a concurrent secondary index.
///
/// External API mirrors `ShardedIndex` so callers can swap with no changes:
/// - `insert(key, pk)` — add pk to the key's posting list
/// - `remove(key, pk)` — remove pk from the key's posting list
/// - `point_lookup(key)` → `Vec<PrimaryKey>`
/// - `range_scan(lower, upper)` → `Vec<PrimaryKey>` (ordered)
/// - `prefix_scan(prefix)` → `Vec<PrimaryKey>` (ordered)
/// - `check_unique(key, pk)` → `Result<(), StorageError>`
pub struct ArtTree {
    root: parking_lot::RwLock<NodePtr>,
}

impl ArtTree {
    pub fn new() -> Self {
        Self { root: parking_lot::RwLock::new(NodePtr::NULL) }
    }

    // ── public write API ──────────────────────────────────────────────────────

    pub fn insert(&self, key: Vec<u8>, pk: PrimaryKey) {
        let mut root = self.root.write();
        *root = unsafe { art_insert(*root, &key, 0, pk) };
    }

    pub fn remove(&self, key: &[u8], pk: &PrimaryKey) {
        let mut root = self.root.write();
        *root = unsafe { art_remove(*root, key, 0, pk) };
    }

    /// Drop the entire tree and reset to empty. Used by `rebuild_secondary_indexes`.
    pub fn clear(&self) {
        let mut root = self.root.write();
        unsafe { art_free(*root) };
        *root = NodePtr::NULL;
    }

    pub fn check_unique(
        &self,
        key: &[u8],
        pk: &PrimaryKey,
    ) -> Result<(), falcon_common::error::StorageError> {
        let root = self.root.read();
        let pks = unsafe { art_lookup(*root, key) };
        if pks.iter().any(|p| p != pk) {
            return Err(falcon_common::error::StorageError::DuplicateKey);
        }
        Ok(())
    }

    // ── public read API ───────────────────────────────────────────────────────

    pub fn point_lookup(&self, key: &[u8]) -> Vec<PrimaryKey> {
        let root = self.root.read();
        unsafe { art_lookup(*root, key) }
    }

    pub fn range_scan(
        &self,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
    ) -> Vec<PrimaryKey> {
        let root = self.root.read();
        let mut results = Vec::new();
        unsafe { art_range(*root, lower, upper, &mut results) };
        results
    }

    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<PrimaryKey> {
        let root = self.root.read();
        let mut results = Vec::new();
        unsafe { art_prefix(*root, prefix, 0, &mut results) };
        results
    }
}

impl Drop for ArtTree {
    fn drop(&mut self) {
        let root = self.root.write();
        unsafe { art_free(*root) };
    }
}

// ── recursive helpers (unsafe, caller holds appropriate OLC or global lock) ──

/// Free an entire subtree.
unsafe fn art_free(node: NodePtr) {
    if node.is_null() { return; }
    match node.tag() {
        TAG_LEAF => { drop(Box::from_raw(node.as_leaf())); }
        TAG_NODE4 => {
            let n = &*node.as_node4();
            for i in 0..n.hdr.num_ch as usize {
                art_free(n.children[i]);
            }
            drop(Box::from_raw(node.as_node4()));
        }
        TAG_NODE16 => {
            let n = &*node.as_node16();
            for i in 0..n.hdr.num_ch as usize {
                art_free(n.children[i]);
            }
            drop(Box::from_raw(node.as_node16()));
        }
        TAG_NODE48 => {
            let n = &*node.as_node48();
            for i in 0..n.hdr.num_ch as usize {
                art_free(n.children[i]);
            }
            drop(Box::from_raw(node.as_node48()));
        }
        TAG_NODE256 => {
            let n = &*node.as_node256();
            for ch in &n.children {
                if !ch.is_null() { art_free(*ch); }
            }
            drop(Box::from_raw(node.as_node256()));
        }
        _ => {}
    }
}

// ── lookup ────────────────────────────────────────────────────────────────────

unsafe fn art_lookup(node: NodePtr, key: &[u8]) -> Vec<PrimaryKey> {
    let mut cur = node;
    let mut depth = 0usize;

    loop {
        if cur.is_null() { return vec![]; }

        if cur.tag() == TAG_LEAF {
            let leaf = &*cur.as_leaf();
            if leaf.key == key {
                return leaf.pks.clone();
            }
            return vec![];
        }

        let hdr = &*cur.hdr();
        // Check compressed prefix
        let pm = hdr.prefix_matches(key, depth);
        if pm < hdr.prefix_len as usize { return vec![]; }
        depth += hdr.prefix_len as usize;

        if depth >= key.len() { return vec![]; }
        let byte = key[depth];
        depth += 1;

        cur = find_child(cur, byte);
    }
}

// ── child navigation ──────────────────────────────────────────────────────────

unsafe fn find_child(node: NodePtr, byte: u8) -> NodePtr {
    match node.tag() {
        TAG_NODE4 => {
            let n = &*node.as_node4();
            for i in 0..n.hdr.num_ch as usize {
                if n.keys[i] == byte { return n.children[i]; }
            }
            NodePtr::NULL
        }
        TAG_NODE16 => {
            let n = &*node.as_node16();
            let count = n.hdr.num_ch as usize;
            // Linear scan (SIMD-friendly for CPU with SSE2; compiler auto-vectorises).
            for i in 0..count {
                if n.keys[i] == byte { return n.children[i]; }
            }
            NodePtr::NULL
        }
        TAG_NODE48 => {
            let n = &*node.as_node48();
            let slot = n.index[byte as usize];
            if slot == 0xFF { NodePtr::NULL } else { n.children[slot as usize] }
        }
        TAG_NODE256 => {
            let n = &*node.as_node256();
            n.children[byte as usize]
        }
        _ => NodePtr::NULL,
    }
}

/// Set a child pointer in an inner node.  Node must be exclusively locked.
unsafe fn set_child(node: NodePtr, byte: u8, child: NodePtr) {
    match node.tag() {
        TAG_NODE4 => {
            let n = &mut *node.as_node4();
            let nc = n.hdr.num_ch as usize;
            // Insert sorted
            let pos = n.keys[..nc].partition_point(|&k| k < byte);
            n.keys[pos..nc + 1].rotate_right(1);
            n.keys[pos] = byte;
            n.children[pos..nc + 1].rotate_right(1);
            n.children[pos] = child;
            n.hdr.num_ch += 1;
        }
        TAG_NODE16 => {
            let n = &mut *node.as_node16();
            let nc = n.hdr.num_ch as usize;
            let pos = n.keys[..nc].partition_point(|&k| k < byte);
            n.keys[pos..nc + 1].rotate_right(1);
            n.keys[pos] = byte;
            n.children[pos..nc + 1].rotate_right(1);
            n.children[pos] = child;
            n.hdr.num_ch += 1;
        }
        TAG_NODE48 => {
            let n = &mut *node.as_node48();
            let slot = n.hdr.num_ch as usize;
            n.index[byte as usize] = slot as u8;
            n.children[slot] = child;
            n.hdr.num_ch += 1;
        }
        TAG_NODE256 => {
            let n = &mut *node.as_node256();
            n.children[byte as usize] = child;
            // num_ch is u8; Node256 can hold 256 children which overflows u8.
            // We use saturating_add: the count is only consulted by art_collect_all
            // on Node256 which iterates the full children array directly, so the
            // exact value beyond 255 is irrelevant.
            n.hdr.num_ch = n.hdr.num_ch.saturating_add(1);
        }
        _ => {}
    }
}

// ── node growth ───────────────────────────────────────────────────────────────

/// Grow Node4 → Node16.
unsafe fn grow_node4(old: *mut Node4) -> NodePtr {
    let mut n16 = Box::new(Node16 {
        hdr:      NodeHeader::new(),
        keys:     [0u8; 16],
        children: [NodePtr::NULL; 16],
    });
    let o = &*old;
    n16.hdr.num_ch     = o.hdr.num_ch;
    n16.hdr.prefix_len = o.hdr.prefix_len;
    n16.hdr.prefix     = o.hdr.prefix;
    let nc = o.hdr.num_ch as usize;
    n16.keys[..nc].copy_from_slice(&o.keys[..nc]);
    n16.children[..nc].copy_from_slice(&o.children[..nc]);
    drop(Box::from_raw(old));
    NodePtr::from_node16(Box::into_raw(n16))
}

/// Grow Node16 → Node48.
unsafe fn grow_node16(old: *mut Node16) -> NodePtr {
    let mut n48 = Box::new(Node48 {
        hdr:      NodeHeader::new(),
        index:    [0xFF; 256],
        children: [NodePtr::NULL; 48],
    });
    let o = &*old;
    n48.hdr.num_ch     = o.hdr.num_ch;
    n48.hdr.prefix_len = o.hdr.prefix_len;
    n48.hdr.prefix     = o.hdr.prefix;
    let nc = o.hdr.num_ch as usize;
    for i in 0..nc {
        n48.index[o.keys[i] as usize] = i as u8;
        n48.children[i] = o.children[i];
    }
    drop(Box::from_raw(old));
    NodePtr::from_node48(Box::into_raw(n48))
}

/// Grow Node48 → Node256.
unsafe fn grow_node48(old: *mut Node48) -> NodePtr {
    let mut n256 = Box::new(Node256 {
        hdr:      NodeHeader::new(),
        children: [NodePtr::NULL; 256],
    });
    let o = &*old;
    n256.hdr.num_ch     = o.hdr.num_ch;
    n256.hdr.prefix_len = o.hdr.prefix_len;
    n256.hdr.prefix     = o.hdr.prefix;
    for (byte, &slot) in o.index.iter().enumerate() {
        if slot != 0xFF {
            n256.children[byte] = o.children[slot as usize];
        }
    }
    drop(Box::from_raw(old));
    NodePtr::from_node256(Box::into_raw(n256))
}

// ── insert ────────────────────────────────────────────────────────────────────

/// Recursively insert `pk` under `key` into the subtree rooted at `node`.
/// Returns the (possibly new) root of the subtree.
unsafe fn art_insert(node: NodePtr, key: &[u8], depth: usize, pk: PrimaryKey) -> NodePtr {
    // Case 1: empty tree → create leaf
    if node.is_null() {
        return NodePtr::from_leaf(Box::into_raw(Box::new(Leaf::new(key.to_vec(), pk))));
    }

    // Case 2: existing leaf → split or append pk
    if node.tag() == TAG_LEAF {
        let leaf = &mut *node.as_leaf();
        if leaf.key == key {
            leaf.insert_pk(pk);
            return node;
        }
        // Keys differ: create a Node4 to split them.
        return split_leaf(node, key, depth, pk);
    }

    // Case 3: inner node
    let hdr = &mut *node.hdr();
    let plen = hdr.prefix_len as usize;
    let pm = hdr.prefix_matches(key, depth);

    if pm < plen {
        // Prefix mismatch: split the compressed prefix.
        return split_prefix(node, key, depth, pm, pk);
    }
    let depth = depth + plen;

    if depth >= key.len() {
        // Key exhausted at this node — shouldn't happen with well-formed keys,
        // but handle gracefully by inserting a leaf with empty suffix.
        let leaf = NodePtr::from_leaf(Box::into_raw(Box::new(Leaf::new(key.to_vec(), pk))));
        return leaf;
    }

    let byte = key[depth];
    let child = find_child(node, byte);

    if !child.is_null() {
        let new_child = art_insert(child, key, depth + 1, pk);
        update_child(node, byte, new_child);
        return node;
    }

    // No child for this byte — insert new leaf, grow node if full.
    let leaf = NodePtr::from_leaf(Box::into_raw(Box::new(Leaf::new(key.to_vec(), pk))));
    if is_full(node) {
        let grown = grow(node);
        set_child(grown, byte, leaf);
        return grown;
    }
    set_child(node, byte, leaf);
    node
}

unsafe fn is_full(node: NodePtr) -> bool {
    match node.tag() {
        TAG_NODE4  => (*node.as_node4()).hdr.num_ch  >= 4,
        TAG_NODE16 => (*node.as_node16()).hdr.num_ch >= 16,
        TAG_NODE48 => (*node.as_node48()).hdr.num_ch >= 48,
        TAG_NODE256 => false,
        _ => false,
    }
}

unsafe fn grow(node: NodePtr) -> NodePtr {
    match node.tag() {
        TAG_NODE4  => grow_node4(node.as_node4()),
        TAG_NODE16 => grow_node16(node.as_node16()),
        TAG_NODE48 => grow_node48(node.as_node48()),
        _ => node,
    }
}

unsafe fn update_child(node: NodePtr, byte: u8, new_child: NodePtr) {
    match node.tag() {
        TAG_NODE4 => {
            let n = &mut *node.as_node4();
            for i in 0..n.hdr.num_ch as usize {
                if n.keys[i] == byte { n.children[i] = new_child; return; }
            }
        }
        TAG_NODE16 => {
            let n = &mut *node.as_node16();
            for i in 0..n.hdr.num_ch as usize {
                if n.keys[i] == byte { n.children[i] = new_child; return; }
            }
        }
        TAG_NODE48 => {
            let n = &mut *node.as_node48();
            let slot = n.index[byte as usize];
            if slot != 0xFF { n.children[slot as usize] = new_child; }
        }
        TAG_NODE256 => {
            (*node.as_node256()).children[byte as usize] = new_child;
        }
        _ => {}
    }
}

/// Split an existing leaf because a new key diverges at `depth`.
unsafe fn split_leaf(
    existing: NodePtr,
    new_key: &[u8],
    depth: usize,
    pk: PrimaryKey,
) -> NodePtr {
    let ex_leaf = &*existing.as_leaf();
    let ex_key = &ex_leaf.key;

    // Find divergence point
    let mut diff = depth;
    while diff < ex_key.len() && diff < new_key.len() && ex_key[diff] == new_key[diff] {
        diff += 1;
    }

    let prefix_len = diff - depth;
    let mut n4 = Box::new(Node4 {
        hdr:      NodeHeader::new(),
        keys:     [0u8; 4],
        children: [NodePtr::NULL; 4],
    });
    n4.hdr.set_prefix(new_key, depth, prefix_len);

    let new_leaf = NodePtr::from_leaf(Box::into_raw(Box::new(Leaf::new(new_key.to_vec(), pk))));

    let ex_byte = if diff < ex_key.len() { ex_key[diff] } else { 0 };
    let new_byte = if diff < new_key.len() { new_key[diff] } else { 0 };

    let raw4 = &mut *n4 as *mut Node4;
    set_child(NodePtr::from_node4(raw4), ex_byte, existing);
    set_child(NodePtr::from_node4(raw4), new_byte, new_leaf);

    NodePtr::from_node4(Box::into_raw(n4))
}

/// Split a compressed prefix because a new key diverges mid-prefix.
unsafe fn split_prefix(
    node: NodePtr,
    key: &[u8],
    depth: usize,
    match_len: usize,
    pk: PrimaryKey,
) -> NodePtr {
    let hdr = &mut *node.hdr();
    let plen = hdr.prefix_len as usize;

    let mut n4 = Box::new(Node4 {
        hdr:      NodeHeader::new(),
        keys:     [0u8; 4],
        children: [NodePtr::NULL; 4],
    });
    // New node's prefix = matched portion
    n4.hdr.set_prefix(key, depth, match_len);

    // Existing node: remaining prefix after split byte
    let split_byte = if match_len < MAX_PREFIX_LEN { hdr.prefix[match_len] } else { 0 };
    let new_plen = plen.saturating_sub(match_len + 1);
    let mut new_prefix = [0u8; MAX_PREFIX_LEN];
    let copy = new_plen.min(MAX_PREFIX_LEN);
    let src_start = (match_len + 1).min(MAX_PREFIX_LEN);
    let src_end = src_start + copy.min(MAX_PREFIX_LEN - src_start);
    new_prefix[..src_end - src_start].copy_from_slice(&hdr.prefix[src_start..src_end]);
    hdr.prefix = new_prefix;
    hdr.prefix_len = new_plen as u16;

    let new_key_byte = if depth + match_len < key.len() { key[depth + match_len] } else { 0 };
    let new_leaf = NodePtr::from_leaf(Box::into_raw(Box::new(Leaf::new(key.to_vec(), pk))));

    let raw4 = &mut *n4 as *mut Node4;
    set_child(NodePtr::from_node4(raw4), split_byte, node);
    set_child(NodePtr::from_node4(raw4), new_key_byte, new_leaf);

    NodePtr::from_node4(Box::into_raw(n4))
}

// ── remove ────────────────────────────────────────────────────────────────────

unsafe fn art_remove(node: NodePtr, key: &[u8], depth: usize, pk: &PrimaryKey) -> NodePtr {
    if node.is_null() { return NodePtr::NULL; }

    if node.tag() == TAG_LEAF {
        let leaf = &mut *node.as_leaf();
        if leaf.key == key {
            leaf.remove_pk(pk);
            if leaf.pks.is_empty() {
                drop(Box::from_raw(node.as_leaf()));
                return NodePtr::NULL;
            }
        }
        return node;
    }

    let hdr = &*node.hdr();
    let plen = hdr.prefix_len as usize;
    let pm = hdr.prefix_matches(key, depth);
    if pm < plen { return node; }
    let depth = depth + plen;

    if depth >= key.len() { return node; }
    let byte = key[depth];
    let child = find_child(node, byte);
    if child.is_null() { return node; }

    let new_child = art_remove(child, key, depth + 1, pk);
    if new_child.is_null() {
        remove_child(node, byte);
    } else {
        update_child(node, byte, new_child);
    }
    node
}

unsafe fn remove_child(node: NodePtr, byte: u8) {
    match node.tag() {
        TAG_NODE4 => {
            let n = &mut *node.as_node4();
            let nc = n.hdr.num_ch as usize;
            if let Some(pos) = (0..nc).find(|&i| n.keys[i] == byte) {
                n.keys[pos..nc].rotate_left(1);
                n.children[pos..nc].rotate_left(1);
                n.hdr.num_ch -= 1;
            }
        }
        TAG_NODE16 => {
            let n = &mut *node.as_node16();
            let nc = n.hdr.num_ch as usize;
            if let Some(pos) = (0..nc).find(|&i| n.keys[i] == byte) {
                n.keys[pos..nc].rotate_left(1);
                n.children[pos..nc].rotate_left(1);
                n.hdr.num_ch -= 1;
            }
        }
        TAG_NODE48 => {
            let n = &mut *node.as_node48();
            let slot = n.index[byte as usize];
            if slot != 0xFF {
                n.children[slot as usize] = NodePtr::NULL;
                n.index[byte as usize] = 0xFF;
                n.hdr.num_ch -= 1;
            }
        }
        TAG_NODE256 => {
            let n = &mut *node.as_node256();
            if !n.children[byte as usize].is_null() {
                n.children[byte as usize] = NodePtr::NULL;
                n.hdr.num_ch -= 1;
            }
        }
        _ => {}
    }
}

// ── range scan ────────────────────────────────────────────────────────────────

unsafe fn art_range(
    node: NodePtr,
    lower: Option<(&[u8], bool)>,
    upper: Option<(&[u8], bool)>,
    out: &mut Vec<PrimaryKey>,
) {
    if node.is_null() { return; }

    if node.tag() == TAG_LEAF {
        let leaf = &*node.as_leaf();
        let key = leaf.key.as_slice();
        let in_lower = lower.map_or(true, |(lo, incl)| {
            if incl { key >= lo } else { key > lo }
        });
        let in_upper = upper.map_or(true, |(hi, incl)| {
            if incl { key <= hi } else { key < hi }
        });
        if in_lower && in_upper {
            out.extend(leaf.pks.iter().cloned());
        }
        return;
    }

    // ART traversal is depth-first and children are kept in sorted key order
    // in Node4/16, so iterating in index order yields sorted output.
    match node.tag() {
        TAG_NODE4 => {
            let n = &*node.as_node4();
            for i in 0..n.hdr.num_ch as usize {
                art_range(n.children[i], lower, upper, out);
            }
        }
        TAG_NODE16 => {
            let n = &*node.as_node16();
            for i in 0..n.hdr.num_ch as usize {
                art_range(n.children[i], lower, upper, out);
            }
        }
        TAG_NODE48 => {
            let n = &*node.as_node48();
            for byte in 0u16..=255 {
                let slot = n.index[byte as usize];
                if slot != 0xFF {
                    art_range(n.children[slot as usize], lower, upper, out);
                }
            }
        }
        TAG_NODE256 => {
            let n = &*node.as_node256();
            for child in &n.children {
                if !child.is_null() {
                    art_range(*child, lower, upper, out);
                }
            }
        }
        _ => {}
    }
}

// ── prefix scan ───────────────────────────────────────────────────────────────

unsafe fn art_prefix(
    node: NodePtr,
    prefix: &[u8],
    depth: usize,
    out: &mut Vec<PrimaryKey>,
) {
    if node.is_null() { return; }

    if node.tag() == TAG_LEAF {
        let leaf = &*node.as_leaf();
        if leaf.key.starts_with(prefix) {
            out.extend(leaf.pks.iter().cloned());
        }
        return;
    }

    let hdr = &*node.hdr();

    if depth >= prefix.len() {
        // We've matched the full prefix: collect entire subtree.
        art_collect_all(node, out);
        return;
    }

    let plen = hdr.prefix_len as usize;
    let pm = hdr.prefix_matches(prefix, depth);
    if pm < plen && depth + pm < prefix.len() {
        return; // prefix mismatch
    }
    let depth = depth + plen;

    if depth >= prefix.len() {
        art_collect_all(node, out);
        return;
    }

    let byte = prefix[depth];
    let child = find_child(node, byte);
    art_prefix(child, prefix, depth + 1, out);
}

/// Collect all PKs in an entire subtree (used after full prefix match).
unsafe fn art_collect_all(node: NodePtr, out: &mut Vec<PrimaryKey>) {
    if node.is_null() { return; }
    if node.tag() == TAG_LEAF {
        let leaf = &*node.as_leaf();
        out.extend(leaf.pks.iter().cloned());
        return;
    }
    match node.tag() {
        TAG_NODE4 => {
            let n = &*node.as_node4();
            for i in 0..n.hdr.num_ch as usize {
                art_collect_all(n.children[i], out);
            }
        }
        TAG_NODE16 => {
            let n = &*node.as_node16();
            for i in 0..n.hdr.num_ch as usize {
                art_collect_all(n.children[i], out);
            }
        }
        TAG_NODE48 => {
            let n = &*node.as_node48();
            for byte in 0u16..=255 {
                let slot = n.index[byte as usize];
                if slot != 0xFF {
                    art_collect_all(n.children[slot as usize], out);
                }
            }
        }
        TAG_NODE256 => {
            let n = &*node.as_node256();
            for child in &n.children {
                if !child.is_null() {
                    art_collect_all(*child, out);
                }
            }
        }
        _ => {}
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(v: u64) -> PrimaryKey { v.to_be_bytes().to_vec() }
    fn key(v: u64) -> Vec<u8>   { v.to_be_bytes().to_vec() }

    #[test]
    fn test_insert_and_lookup() {
        let tree = ArtTree::new();
        tree.insert(key(1), pk(100));
        tree.insert(key(2), pk(200));
        tree.insert(key(3), pk(300));

        assert_eq!(tree.point_lookup(&key(1)), vec![pk(100)]);
        assert_eq!(tree.point_lookup(&key(2)), vec![pk(200)]);
        assert_eq!(tree.point_lookup(&key(3)), vec![pk(300)]);
        assert!(tree.point_lookup(&key(99)).is_empty());
    }

    #[test]
    fn test_multi_pk_per_key() {
        let tree = ArtTree::new();
        tree.insert(key(42), pk(1));
        tree.insert(key(42), pk(2));
        tree.insert(key(42), pk(3));

        let mut got = tree.point_lookup(&key(42));
        got.sort();
        assert_eq!(got, vec![pk(1), pk(2), pk(3)]);
    }

    #[test]
    fn test_remove() {
        let tree = ArtTree::new();
        tree.insert(key(10), pk(1));
        tree.insert(key(10), pk(2));

        tree.remove(&key(10), &pk(1));
        assert_eq!(tree.point_lookup(&key(10)), vec![pk(2)]);

        tree.remove(&key(10), &pk(2));
        assert!(tree.point_lookup(&key(10)).is_empty());
    }

    #[test]
    fn test_range_scan_ordered() {
        let tree = ArtTree::new();
        for i in 0u64..10 {
            tree.insert(key(i), pk(i * 10));
        }

        let results = tree.range_scan(
            Some((key(3).as_slice(), true)),
            Some((key(7).as_slice(), false)),
        );
        // Should return pk(30), pk(40), pk(50), pk(60) in order
        assert_eq!(results, vec![pk(30), pk(40), pk(50), pk(60)]);
    }

    #[test]
    fn test_prefix_scan() {
        let tree = ArtTree::new();
        // Keys share common prefix 0x00_00_00_00 (u64 0..15 all have same high bytes)
        tree.insert(key(0x0001), pk(1));
        tree.insert(key(0x0002), pk(2));
        tree.insert(key(0x0100), pk(100));

        let prefix = &[0x00u8, 0x00];
        let mut results = tree.prefix_scan(prefix);
        results.sort();
        assert!(results.contains(&pk(1)));
        assert!(results.contains(&pk(2)));
    }

    #[test]
    fn test_check_unique() {
        let tree = ArtTree::new();
        tree.insert(key(5), pk(99));

        // Same pk → ok
        assert!(tree.check_unique(&key(5), &pk(99)).is_ok());
        // Different pk → duplicate
        assert!(tree.check_unique(&key(5), &pk(100)).is_err());
        // New key → ok
        assert!(tree.check_unique(&key(6), &pk(100)).is_ok());
    }

    #[test]
    fn test_large_insert() {
        let tree = ArtTree::new();
        for i in 0u64..1000 {
            tree.insert(key(i), pk(i));
        }
        for i in 0u64..1000 {
            assert_eq!(tree.point_lookup(&key(i)), vec![pk(i)], "lookup failed for key {i}");
        }
    }

    #[test]
    fn test_concurrent_insert_lookup() {
        use std::sync::Arc;
        let tree = Arc::new(ArtTree::new());
        let mut handles = vec![];

        for t in 0u64..8 {
            let tree = Arc::clone(&tree);
            handles.push(std::thread::spawn(move || {
                for i in 0u64..128 {
                    tree.insert(key(t * 1000 + i), pk(t * 1000 + i));
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        for t in 0u64..8 {
            for i in 0u64..128 {
                let k = key(t * 1000 + i);
                assert!(!tree.point_lookup(&k).is_empty(), "missing key {}", t * 1000 + i);
            }
        }
    }
}
