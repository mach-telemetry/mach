# Notes from reading source code

- `List`
  - contains:
    - `InnerList`
      - contains: wp-lock-protected head node, has_writer
        - `Node`
          - chunk_id: many nodes belong to a chunk
          - offset: how far into the chunk is this node
          - size: how much data does this node hold
          - buffer_id: ?
    - `ListBuffer`
      - this is the current buffer?
      - arc<wplock<innerbuffer>>
        - `InnerBuffer`
          - push_segment() -> Node
          - push_bytes() -> Node
          - read()
- `ListWriter` 
  - contains cloned inner list and list buffer
- `ListReader`

## Notes

- `chunk_id`, in `InnerBuffer`, is updated when the current chunk
  is flushed.

## Questions

- `InnerBuffer`
  - inner buffer has a chunk ID property, why require `chunk_id`
    as a parameter to push_segment() and push_bytes()?


----------------
# 2/10/22 meeting w/ Franco


