#ifndef __ZDB_INDEX_SEQ_H
    #define __ZDB_INDEX_SEQ_H

    index_seqmap_t *index_fileid_from_seq(index_root_t *root, uint32_t seqid);
    void index_seqid_push(index_root_t *root, uint32_t id, uint16_t indexid);
    size_t index_seq_offset(uint32_t relative);

    void index_seqid_dump(index_root_t *root);
#endif
