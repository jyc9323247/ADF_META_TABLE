

```sql
-- META.TB_META_MINER_TERM 수집쿼리

SELECT MU.UFW_NM AS WORD_NM, 
	MUD.UFW_DIC_NM AS WORD_DICTIONARY_NM, 
	MU.PHSC_NM AS ENGNM, 
	MU.UFW_DEF AS WORD_DEFN_CONT, 
	MDS.DAT_STRC_NM AS DMIN_NM,
	CURRENT TIMESTAMP AS IPDTM

FROM MM_UFW MU
LEFT JOIN MM_UFW_DIC MUD ON MU.UFW_DIC_ID = MUD.UFW_DIC_ID
LEFT JOIN MM_DAT_STRC MDS ON MDS.DAT_STRC_ID = MU.DAT_STRC_ID

WHERE MU.DEL_YN = 'N' 
ORDER BY MUD.UFW_DIC_NM, MU.UFW_NM
;
```

```sql
-- META.TB_META_MINER_CODE 수집쿼리
  
SELECT 	
	MCD.MNGM_ID AS CD_ID, 
	MCD.CODE_DOM_NM AS CD_NM, 
	MCD.CODE_VAL_LEN AS CD_LEN_VL,
	MCD.CODE_DOM_DEF AS CD_DEFN_CONT,
	CASE
		WHEN MCD.CODE_TP_CD = 'CDTP_GEN' THEN '일반코드'
		WHEN MCD.CODE_TP_CD = 'CDTP_COB' THEN '참조코드'
		ELSE MCD.CODE_TP_CD
	END AS CD_DESC, 
	MCDV.CODE_VAL_NM AS CD_VL_NM,
	MCDV.CODE_VAL AS CD_VL,
	MCDV.CODE_VAL_DEF AS CD_VL_DEFN_CONT,
	MCDV.DISP_ODR AS CD_SORT_RNKN,
	UV.UDP_VAL AS CD_TYP_NM,
	CURRENT TIMESTAMP AS IPDTM

FROM MM_CODE_DOM MCD 
LEFT JOIN MM_CODE_DOM_VAL MCDV ON MCD.CODE_DOM_ID = MCDV.CODE_DOM_ID
LEFT JOIN MM_UDP_VAL UV ON MCD.CODE_DOM_ID = UV.TRG_ID
WHERE MCD.UFW_DIC_ID = '2c908079/184ee4/0118/4ee38daf/0001' 

ORDER BY MCDV.DISP_ODR
;
```

```sql
-- META.TB_META_MINER_TABLE 수집쿼리

SELECT ME.ENTITY_NAME AS TBL_NM, 
     	ME."DEFINITION" AS TBL_HNNM,   /* 수정필요 */
 	MDS.DAT_STRC_NAMESPACE AS DMIN_NM, 
	MLI.LNK_INFO_NM AS SRCE_SYS_NM, 
	MLI.LNK_INFO_NM AS SCHEMA_NM,     /* 수정필요 */
	MLI.LNK_INFO_NM AS CTLG_SCHEMA_NM,     /* 수정필요 */
	ME."DEFINITION" AS TBL_DTL_DESC_CONT, 
	ME.REQ_DT AS FRST_RGST_DTM, 
	ME.UPD_DT AS MDF_DTNT,
	CASE
		WHEN MUV1.UDP_VAL IS NOT NULL THEN MUV1.UDP_VAL
		ELSE '영구'  -- 기본값
	END AS DATA_CUSD_PRID_CONT,
	CASE 
		WHEN MUV2.UDP_VAL IS NOT NULL THEN MUV2.UDP_VAL
		ELSE 'N'     -- 기본값
	END AS SPRT_CUST_YN,
	CASE
		WHEN EXISTS (
			SELECT 1
			FROM MM_ATTRIBUTE B
			INNER JOIN MM_UFW A
				ON B.LOGICAL_ATTRIBUTE_NAME = A.UFW_NM AND B.PHYSICAL_ATTRIBUTE_NAME = A.PHSC_NM
			WHERE B.ENTITY_ID = ME.ENTITY_ID
				AND A.UFW_ID IN (SELECT TRG_ID FROM MM_UDP_VAL WHERE UDP_ID IN ('K1565162437180', 'K1644905984798', 'K1678171958382') AND UDP_VAL IN ('1', '2', '3', '4'))
			) THEN 'Y'
		ELSE 'N'
	END AS PRSN_INFO_ICYN,
	(SELECT MIN(V.UDP_VAL) FROM MM_ATTRIBUTE B
		JOIN MM_UFW A ON B.LOGICAL_ATTRIBUTE_NAME = A.UFW_NM AND B.PHYSICAL_ATTRIBUTE_NAME = A.PHSC_NM
		JOIN MM_UDP_VAL V ON A.UFW_ID = V.TRG_ID
		WHERE B.ENTITY_ID = ME.ENTITY_ID AND V.UDP_ID IN ('K1565162437180', 'K1644905984798', 'K1678171958382') AND UDP_VAL IN ('1', '2', '3', '4') ) AS PRSN_INFO_DATA_LVL_GRDE,
	MUV3.UDP_VAL AS PRSN_INFO_DISPOSAL_CYCL_CONT,
	MUV4.UDP_VAL AS SPRT_CUSD_BASE_DESC_CONT,
	MUV9.UDP_VAL AS FRST_TBL_OWNM,
	MUV8.UDP_VAL AS FRST_REQS_SVC_REQ_ID,
	MUV5.UDP_VAL AS CHG_REQS_SVC_REQ_ID,
	MUV6.UDP_VAL AS FRST_REQS_EMPNO,
	MUV10.UDP_VAL AS FRST_TBL_OW_TMNM,
	MUV7.UDP_VAL AS DATA_OWNER_DEPT,
        CASE 
		WHEN MUV2.UDP_VAL IS NOT NULL THEN MUV2.UDP_VAL
		ELSE 'N'     -- 기본값
	END AS DISPOSAL_TGT_YN,
        NULL AS SPRT_DISPOSAL_BASE_COL_NM,
        NULL AS DATA_LIFE_CYCL_CONT,
	CURRENT TIMESTAMP AS IPDTM
	FROM MM_ENTITY ME
LEFT JOIN MM_DAT_STRC MDS ON ME.LNK_INFO_ID = MDS.DAT_STRC_ID
LEFT JOIN MM_LNK_INFO MLI ON MLI.LNK_INFO_ID = ME.LNK_INFO_ID 
-- 테이블 보관기간
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD1 ON MDS.DAT_STRC_ID = MUD1.TRG_ID AND MUD1.DISPLAY_NAME = '테이블보관기간'   
LEFT JOIN MM_UDP_VAL MUV1 ON MUV1.TRG_ID = ME.ENTITY_ID AND MUV1.UDP_ID = MUD1.UDP_ID
-- 분리보관대상여부
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD2 ON MDS.DAT_STRC_ID = MUD2.TRG_ID AND MUD2.DISPLAY_NAME = '분리보관대상여부'   
LEFT JOIN MM_UDP_VAL MUV2 ON MUV2.TRG_ID = ME.ENTITY_ID AND MUV2.UDP_ID = MUD2.UDP_ID
-- 개인정보파기주기
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD3 ON MDS.DAT_STRC_ID = MUD3.TRG_ID AND MUD3.DISPLAY_NAME = '개인정보파기주기'   
LEFT JOIN MM_UDP_VAL MUV3 ON MUV3.TRG_ID = ME.ENTITY_ID AND MUV3.UDP_ID = MUD3.UDP_ID
-- 분리보관기준
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD4 ON MDS.DAT_STRC_ID = MUD4.TRG_ID AND MUD4.DISPLAY_NAME = '분리보관기준'   
LEFT JOIN MM_UDP_VAL MUV4 ON MUV4.TRG_ID = ME.ENTITY_ID AND MUV4.UDP_ID = MUD4.UDP_ID
-- 변경신청SR번호
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD5 ON MDS.DAT_STRC_ID = MUD5.TRG_ID AND MUD5.DISPLAY_NAME = '변경신청SR번호'   
LEFT JOIN MM_UDP_VAL MUV5 ON MUV5.TRG_ID = ME.ENTITY_ID AND MUV5.UDP_ID = MUD5.UDP_ID
-- 최초 신청자 사번
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD6 ON MDS.DAT_STRC_ID = MUD6.TRG_ID AND MUD6.DISPLAY_NAME = '최초 신청자 사번'
LEFT JOIN MM_UDP_VAL MUV6 ON MUV6.TRG_ID = ME.ENTITY_ID AND MUV6.UDP_ID = MUD6.UDP_ID
-- 데이터오너부서
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD7 ON MDS.DAT_STRC_ID = MUD7.TRG_ID AND MUD7.DISPLAY_NAME = '데이터오너부서'
LEFT JOIN MM_UDP_VAL MUV7 ON MUV7.TRG_ID = ME.ENTITY_ID AND MUV7.UDP_ID = MUD7.UDP_ID
-- 최초신청SR번호
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD8 ON MDS.DAT_STRC_ID = MUD8.TRG_ID AND MUD8.DISPLAY_NAME = '최초신청SR번호'
LEFT JOIN MM_UDP_VAL MUV8 ON MUV8.TRG_ID = ME.ENTITY_ID AND MUV8.UDP_ID = MUD8.UDP_ID
-- 최초신청자명 (최초 테이블 소유자)
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD9 ON MDS.DAT_STRC_ID = MUD9.TRG_ID AND MUD9.DISPLAY_NAME = '최초신청자명'
LEFT JOIN MM_UDP_VAL MUV9 ON MUV9.TRG_ID = ME.ENTITY_ID AND MUV9.UDP_ID = MUD9.UDP_ID
-- 최초 팀명  (최초 테이블 소유 팀)
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD10 ON MDS.DAT_STRC_ID = MUD10.TRG_ID AND MUD10.DISPLAY_NAME = '최초 팀명'
LEFT JOIN MM_UDP_VAL MUV10 ON MUV10.TRG_ID = ME.ENTITY_ID AND MUV10.UDP_ID = MUD10.UDP_ID
WHERE ME.DEL_YN = 'N' AND MDS.DAT_STRC_NM IS NOT NULL
ORDER BY MDS.DAT_STRC_NM, ME.ENTITY_NAME
;
```



```sql
-- META.TB_META_MINER_COLUMN 수집쿼리

SELECT 	MUV4.UDP_VAL AS SCHEMA_NM,
          MA.ENTITY_NAME AS TBL_NM, 
          MA.ATTRIBUTE_NAME AS COL_NM, 
          MA.LOGICAL_ATTRIBUTE_NAME AS HNGL_CLNM, 
          MDS.DAT_STRC_NAMESPACE AS SYS_NM, 
	MUV3.UDP_VAL AS BLN_TBL_OW_TMNM,
          MA.TYPE_NAME AS COL_TYP_NM, 
          MA.COLUMN_SIZE AS COL_LEN_VL, 
          MA.DECIMAL_DIGITS AS DCML_UNDR_CPHR, 
	MUV1.UDP_VAL AS COL_ENCD_TYP_NM,
	MUV2.UDP_VAL AS COL_ENCD_GRDE,
        CASE WHEN MUV2.UDP_VAL in ('1','2','3','4') THEN 'Y' ELSE 'N' END AS PRSN_INFO_YN,
        CASE WHEN MUV2.UDP_VAL in ('4') THEN 'Y' ELSE 'N' END AS SENS_INFO_YN,
        CASE WHEN MUV2.UDP_VAL in ('1','2','3','4') THEN 'Y' ELSE 'N' END AS MASKING_YN,
	CURRENT TIMESTAMP AS IPDTM
FROM MM_ATTRIBUTE MA
LEFT JOIN MM_DAT_STRC MDS ON MA.LNK_INFO_ID = MDS.DAT_STRC_ID AND MDS.ROOT_YN = 'N'
LEFT JOIN MM_UFW MU ON MA.LOGICAL_ATTRIBUTE_NAME = MU.UFW_NM AND MA.PHYSICAL_ATTRIBUTE_NAME = MU.PHSC_NM AND MDS.UFW_DIC_ID = MU.UFW_DIC_ID
LEFT JOIN MM_UDP_VAL MUV1 ON MUV1.TRG_ID = MU.UFW_ID AND MUV1.UDP_ID IN ('K1678171958346', 'K1644905984734', 'K1565162267523')
LEFT JOIN MM_UDP_VAL MUV2 ON MUV2.TRG_ID = MU.UFW_ID AND MUV2.UDP_ID IN ('K1565162437180', 'K1644905984798', 'K1678171958382')
-- 최종 테이블 소유팀 (데이터오너부서)
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_TB_INFO'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD3 ON MDS.DAT_STRC_ID = MUD3.TRG_ID AND MUD3.DISPLAY_NAME = '데이터오너부서'
LEFT JOIN MM_UDP_VAL MUV3 ON MUV3.TRG_ID = MA.ENTITY_ID AND MUV3.UDP_ID = MUD3.UDP_ID
-- 스키마명 (OWNER)
LEFT JOIN (SELECT A.UDP_ID, A.TRG_ID, A.UDP_TRG_TP_CD, A.FILED_DFT_VAL, A.FILED_SEQ, B.DISPLAY_NAME, C.DAT_STRC_NM
			FROM MM_UDP_DEF A, MM_UDP_DEF_DISPLAY B, MM_DAT_STRC C
			WHERE A.UDP_ID = B.FIELD_NM AND A.UDP_TRG_TP_CD = 'UDTP_DAT_STRC'
			AND A.TRG_ID = C.DAT_STRC_ID
			ORDER BY C.DAT_STRC_NM, A.FILED_SEQ) MUD4 ON MDS.DAT_STRC_ID = MUD4.TRG_ID AND MUD4.DISPLAY_NAME = 'OWNER'
LEFT JOIN MM_UDP_VAL MUV4 ON MUV4.TRG_ID = MUD4.TRG_ID AND MUV4.UDP_ID = MUD4.UDP_ID
WHERE MA.DEL_YN = 'N'
ORDER BY MDS.DAT_STRC_NM, MA.ENTITY_NAME, MA.ATTRIBUTE_NAME
;
```


```sql
--DB2 테이블 정보

-- META.TB_MLCRP_CATALOG_TABLE , META.TB_MLIWP_CATALOG_TABLE, META.TB_MLSQP_CATALOG_TABLE, META.TB_MLCYP_CATALOG_TABLE,
-- META.TB_MLHSP_CATALOG_TABLE, META.TB_MLVOCP_CATALOG_TABLE, META.TB_MLRBAP_CATALOG_TABLE

/* 대상 DB명 :
'MLCRP'
'MLIWP'
'MLSQP'
'MLCYP'
'MLHSP'
'MLRBAP'
'MLVOCP'
*/

select  current server AS SRCE_CTLG_NM, 
 a.tabschema AS SRCE_SCHEMA_NM, 
 a.tabname AS SRCE_TBL_NM, 
 a.npages AS SRCE_TBL_SIZE_VL,
 a.card AS SRCE_TBL_CNT, 
 a.colcount AS ORGN_COL_CNT, 
 b.colnames AS SRCE_TBL_PRIMARY_KY,
 c.datapartitionexpression AS SRCE_TBL_PRTT_KY_CONT,
 a.stats_time AS SRCE_TBL_STTA_DTM, 
 a.lastused AS SRCE_TBL_LST_USEDT,
 a.alter_time AS SRCE_TBL_LST_SCHEMA_CHG_DTM,
 current timestamp AS IPDTM
 from SYSCAT.TABLES a
left outer join SYSCAT.INDEXES b
on a.tabschema=b.tabschema 
and a.tabname=b.tabname
and a.keyindexid=b.iid
left outer join SYSCAT.DATAPARTITIONEXPRESSION c
on a.tabschema=c.tabschema 
and a.tabname=c.tabname
where a.tabschema not like 'SYS%'
;
```

```sql
--Db2 컬럼 정보
-- META.TB_MLCRP_CATALOG_COLUMN , META.TB_MLIWP_CATALOG_COLUMN, META.TB_MLSQP_CATALOG_COLUMN, META.TB_MLCYP_CATALOG_COLUMN, 
-- META.TB_MLHSP_CATALOG_COLUMN, META.TB_MLVOCP_CATALOG_COLUMN, META.TB_MLRBAP_CATALOG_COLUMN


/* 대상 DB명 :
'MLCRP'
'MLIWP'
'MLSQP'
'MLCYP'
'MLHSP'
'MLRBAP'
'MLVOCP'
*/


select 
    current server AS SRCE_CTLG_NM, 
    a.tabschema AS SRCE_SCHEMA_NM, 
    a.tabname AS SRCE_TBL_NM, 
    a.colname AS SRC_COL_NM, 
    a.typename AS COL_TYP_NM, 
    a.length AS COL_LEN_VL, 
    a.scale AS DCML_UNDR_CPHR, 
    a.default AS COL_BASC_VL, 
    a.nulls AS NA_PMN_YN, 
    case when a.keyseq is not null then 'Y' 
    else 'N' end as PRMR_KY_YN, 
    case when c.datapartitionexpression is not null then 'Y' else 'N' end as COL_PRTT_KY_YN, 
    a.colcard AS COL_CARDINALITY_VL, 
    current_timestamp as IPDTM
from SYSCAT.COLUMNS a
left outer join SYSCAT.DATAPARTITIONEXPRESSION c
on a.tabschema=c.tabschema 
and a.tabname=c.tabname
where a.tabschema not like 'SYS%'
with ur;
```



```sql
--PG 테이블정보
--대상 DB : 'MLMSAP'
SELECT  
    current_database() AS SRCE_CTLG_NM, 
    n.nspname AS SRCE_SCHEMA_NM, 
    c.relname AS SRCE_TBL_NM, 
    c.relpages AS SRCE_TBL_SIZE_VL, 
    c.reltuples AS SRCE_TBL_CNT, 
    col.colcount AS ORGN_COL_CNT, 
    pk.pk_columns AS SRCE_TBL_PRIMARY_KY, 
    pg_get_partkeydef(c.oid) AS SRCE_TBL_PRTT_KY_CONT, 
    GREATEST(s.last_analyze, s.last_autoanalyze) AS SRCE_TBL_STTA_DTM,
    NULL::timestamp AS SRCE_TBL_LST_USEDT,
    NULL::timestamp AS SRCE_TBL_LST_SCHEMA_CHG_DTM, 
    current_timestamp AS IPDTM
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace   
LEFT JOIN pg_stat_user_tables s   ON s.relid = c.oid  
LEFT JOIN LATERAL (SELECT COUNT(*) AS colcount  FROM pg_attribute a  WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) col ON true  
LEFT JOIN LATERAL (   SELECT string_agg(a.attname, ',' ORDER BY array_position(con.conkey, a.attnum)) AS pk_columns
          FROM pg_constraint con JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)   
          WHERE con.conrelid = c.oid AND con.contype = 'p') pk ON true 
WHERE c.relkind IN ('r', 'p') AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'  
;
```

```sql
--PG 컬럼 정보
--대상 DB : 'MLMSAP'

SELECT
    current_database() AS SRCE_CTLG_NM, 
    c.table_schema AS SRCE_SCHEMA_NM, 
    c.table_name AS SRCE_TBL_NM, 
    c.column_name AS SRC_COL_NM, 
    c.udt_name AS COL_TYP_NM, 
    COALESCE(c.character_maximum_length, c.numeric_precision) AS COL_LEN_VL, 
    c.numeric_scale AS DCML_UNDR_CPHR, 
    c.column_default AS COL_BASC_VL, 
    CASE WHEN c.is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS NA_PMN_YN, 
    CASE WHEN kcu.ordinal_position IS NOT NULL THEN 'Y' ELSE 'N' END AS PRMR_KY_YN, 
    CASE WHEN pk_col.attnum IS NOT NULL THEN 'Y' ELSE 'N' END AS COL_PRTT_KY_YN, 
    CASE WHEN st.n_distinct >= 0 THEN st.n_distinct::bigint WHEN st.n_distinct < 0 THEN ROUND(- st.n_distinct * cl.reltuples)::bigint END AS COL_CARDINALITY_VL, 
    current_timestamp AS IPDTM
FROM information_schema.columns c   
JOIN pg_class cl ON cl.relname = c.table_name  
JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = c.table_schema  
LEFT JOIN pg_stats st  ON st.schemaname = c.table_schema   AND st.tablename = c.table_name AND st.attname = c.column_name  
LEFT JOIN information_schema.table_constraints tc  ON tc.table_schema = c.table_schema  AND tc.table_name = c.table_name  AND tc.constraint_type = 'PRIMARY KEY'  
LEFT JOIN information_schema.key_column_usage kcu  ON kcu.constraint_name = tc.constraint_name  AND kcu.table_schema = c.table_schema  AND kcu.table_name = c.table_name  AND kcu.column_name = c.column_name  
LEFT JOIN pg_partitioned_table pt  ON pt.partrelid = cl.oid AND pt.partstrat = 'r'  
LEFT JOIN pg_attribute pk_col ON pk_col.attrelid = cl.oid AND pk_col.attname = c.column_name AND pt.partattrs IS NOT NULL AND pk_col.attnum  = ANY(string_to_array(pt.partattrs::text, ' ')::smallint[])  
WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema') AND c.table_schema NOT LIKE 'pg\_toast%' ESCAPE '\'  AND cl.relkind IN ('r', 'p')  
;
```


```sql

--MS 테이블 정보
-- META.TB_MLMCCP_CATALOG_TABLE, META.TB_MLMCSP_CATALOG_TABLE, META.TB_MLADBP_CATALOG_TABLE

/* 대상 DB명 :
'MLMCCP'
'MLMCSP'
'MLADBP'
*/

SELECT  
    DB_NAME() AS SRCE_CTLG_NM,  
    s.name AS SRCE_SCHEMA_NM,
    t.name AS SRCE_TBL_NM,
    ps.used_page_count AS SRCE_TBL_SIZE_VL,
    ps.row_count AS SRCE_TBL_CNT,
    (SELECT COUNT(*) 
     FROM sys.columns c 
     WHERE c.object_id = t.object_id) AS ORGN_COL_CNT,
    pk.colnames AS SRCE_TBL_PRIMARY_KY,
    pc.part_key AS SRCE_TBL_PRTT_KY_CONT,  
    stat.stats_time AS SRCE_TBL_STTA_DTM,
    ius.lastused AS SRCE_TBL_LST_USEDT,
    t.modify_date AS SRCE_TBL_LST_SCHEMA_CHG_DTM,
    current_timestamp AS IPDTM
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
LEFT JOIN (
    SELECT object_id,
           SUM(used_page_count) AS used_page_count,
           SUM(row_count)       AS row_count
    FROM sys.dm_db_partition_stats
    WHERE index_id IN (0,1)   -- heap(0) or clustered index(1)
    GROUP BY object_id
) ps ON ps.object_id = t.object_id
OUTER APPLY (
    SELECT MAX(sp.last_updated) AS stats_time
    FROM sys.stats st
    CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
    WHERE st.object_id = t.object_id
) stat
OUTER APPLY (
    SELECT MAX(v.val) AS lastused
    FROM (VALUES
        ((SELECT MAX(last_user_seek)   FROM sys.dm_db_index_usage_stats WHERE object_id = t.object_id)),
        ((SELECT MAX(last_user_scan)   FROM sys.dm_db_index_usage_stats WHERE object_id = t.object_id)),
        ((SELECT MAX(last_user_lookup) FROM sys.dm_db_index_usage_stats WHERE object_id = t.object_id)),
        ((SELECT MAX(last_user_update) FROM sys.dm_db_index_usage_stats WHERE object_id = t.object_id))
    ) AS v(val)
) ius
OUTER APPLY (
    SELECT STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS colnames
    FROM sys.indexes i
    JOIN sys.index_columns ic
        ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c
        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = t.object_id
      AND i.is_primary_key = 1
) pk
OUTER APPLY (
    SELECT STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.partition_ordinal) AS part_key
    FROM sys.indexes i
    JOIN sys.index_columns ic
        ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.partition_ordinal > 0
    JOIN sys.columns c
        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = t.object_id
) pc
WHERE s.name NOT LIKE 'SYS%';
```

```sql
--MS 컬럼 정보
-- META.TB_MLMCCP_CATALOG_COLUMN, META.TB_MLMCSP_CATALOG_COLUMN, META.TB_MLADBP_CATALOG_COLUMN


/* 대상 DB명 :
'MLMCCP'
'MLMCSP'
'MLADBP'
*/

SELECT  DB_NAME() AS SRCE_CTLG_NM, 
    s.name  AS SRCE_SCHEMA_NM,
    t.name  AS SRCE_TBL_NM,
    c.name  AS SRC_COL_NM,
    ty.name AS COL_TYP_NM,
    c.max_length AS COL_LEN_VL,
    c.scale AS DCML_UNDR_CPHR,
    dc.definition AS COL_BASC_VL,
    CASE WHEN c.is_nullable = 1 THEN 'Y' ELSE 'N' END AS NA_PMN_YN,
    CASE WHEN pk.column_id IS NOT NULL THEN 'Y' ELSE 'N' END AS PRMR_KY_YN,
    CASE WHEN part.column_id IS NOT NULL THEN 'Y' ELSE 'N' END AS COL_PRTT_KY_YN,  
    hist.distinct_count AS COL_CARDINALITY_VL,
    current_timestamp AS IPDTM
FROM sys.columns c
JOIN sys.tables t   ON t.object_id = c.object_id
JOIN sys.schemas s  ON s.schema_id = t.schema_id
JOIN sys.types ty   ON ty.user_type_id = c.user_type_id
LEFT JOIN sys.default_constraints dc
    ON dc.object_id = c.default_object_id
OUTER APPLY (
    SELECT ic.column_id
    FROM sys.indexes i
    JOIN sys.index_columns ic
        ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE i.object_id = c.object_id AND i.is_primary_key = 1
      AND ic.column_id = c.column_id AND ic.key_ordinal = 1
) pk
OUTER APPLY (
    SELECT ic.column_id
    FROM sys.index_columns ic
    WHERE ic.object_id = c.object_id
      AND ic.column_id = c.column_id
      AND ic.partition_ordinal > 0
) part
OUTER APPLY (
    -- 해당 컬럼을 선두 키로 갖는 통계 하나를 찾아 히스토그램에서 distinct count 근사
    SELECT TOP 1 st.stats_id, st.name AS stat_name
    FROM sys.stats st
    JOIN sys.stats_columns sc
        ON sc.object_id = st.object_id AND sc.stats_id = st.stats_id
    WHERE st.object_id = c.object_id
      AND sc.column_id = c.column_id
      AND sc.stats_column_id = 1
) statinfo
OUTER APPLY (
    SELECT SUM(h.distinct_range_rows) + COUNT(*) AS distinct_count
    FROM sys.dm_db_stats_histogram(c.object_id, statinfo.stats_id) h
) hist
WHERE s.name NOT LIKE 'SYS%';
```
