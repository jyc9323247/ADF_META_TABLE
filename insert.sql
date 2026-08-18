
insert
    into
    "ADFADM".ctl_ingest_target_master
(data_class,
    ingest_type,
    src_type,
    src_db_nm,
    src_schema_nm,
    src_table_nm,
    init_wait_yn,
    created_by,
    created_dt)
values('META', 'FULL', 'DB2', 'XMETA', 'SYSCAT', 'TABLES',  'N'::bpchar, 'ADF', now())
,('META', 'FULL', 'DB2', 'XMETA', 'SYSCAT', 'COLUMS',  'N'::bpchar, 'ADF', now());


insert
    into
    "ADFADM".ctl_ingest_target_master
(data_class,
    ingest_type,
    src_type,
    src_db_nm,
    src_schema_nm,
    src_table_nm,
    cdc_tgt_db_nm,
    cdc_tgt_schema_nm,
    partition_clause,
    exec_group,
    delete_yn,
    init_wait_yn,
    created_by,
    created_dt)
values('RAW', 'FULL', 'DB2', 'XMETA', 'DB2INT1', 'LINEITEM', NULL, NULL, '', '', 'N'::bpchar, 'Y'::bpchar, 'ADF', now())
,('RAW', 'FULL', 'DB2', 'XMETA', 'DB2INT1', 'TEST_CLYM', NULL, NULL, '', '', 'N'::bpchar, 'Y'::bpchar, 'ADF', now())
,('RAW', 'FULL', 'DB2', 'XMETA', 'DB2INT1', 'TEST_CLYM', 'MLCRP', 'DAER', '', '', 'N'::bpchar, 'Y'::bpchar, 'ADF', now())
;
