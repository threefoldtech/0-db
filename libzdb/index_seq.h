#ifndef __ZDB_INDEX_SEQ_H
    #define __ZDB_INDEX_SEQ_H

    index_seqmap_t *index_fileid_from_seq(index_root_t *root, seqid_t seqid);
    void index_seqid_push(index_root_t *root, seqid_t id, fileid_t indexid);
    size_t index_seq_offset(seqid_t relative);

    void index_seqid_dump(index_root_t *root);
#endif
