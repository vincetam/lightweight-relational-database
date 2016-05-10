/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL		16
#define MAX_TOK_LEN		32
#define KEYWORD_OFFSET	10
#define STRING_BREAK	" (),<>="
#define NUMBER_BREAK	" ),"

#define LOG_FILENAME	"db.log"
#define STRING_BACKUP   "BACKUP"
#define STRING_RF_START "RF_START"

#define MAX_COL_DISPLAY_LEN 16
#define MAX_CHAR_LEN        255
#define MAX_NUM_RECORD      1000
#define MAX_LOG_ENTRY_LEN	512
#define TIMESTAMP_LEN       14

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int			tpd_size;
	char		table_name[MAX_IDENT_LEN+4];
	int			num_columns;
	int			cd_offset;
	int			tpd_flags;
} tpd_entry;

/* record descriptor structure */
typedef struct rd_entry_def
{
} rd_entry;

/* Table file header structure */
typedef struct table_file_header_def
{
	int			file_size;				// 4 bytes
	int			record_size;			// 4 bytes
	int			num_records;			// 4 bytes
	int			record_offset;			// 4 bytes
	int			file_header_flag;		// 4 bytes
	tpd_entry	*tpd_ptr;				// 4 bytes
} table_file_header;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is 
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int			list_size;
	int			num_tables;
	int			db_flags;
	tpd_entry	tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked 
	 together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

/* This enum defines the different classes of tokens for 
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
  function_name,// 6
	terminator,		// 7
	error			    // 8
  
} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_CHAR,		    // 11 
	K_CREATE, 		// 12
	K_TABLE,			// 13
	K_NOT,				// 14
	K_NULL,				// 15
	K_DROP,				// 16
	K_LIST,				// 17
	K_SCHEMA,			// 18
  K_FOR,        // 19
	K_TO,				  // 20
  K_INSERT,     // 21
  K_INTO,       // 22
  K_VALUES,     // 23
  K_DELETE,     // 24
  K_FROM,       // 25
  K_WHERE,      // 26
  K_UPDATE,     // 27
  K_SET,        // 28
  K_SELECT,     // 29
  K_ORDER,      // 30
  K_BY,         // 31
  K_DESC,       // 32
  K_IS,         // 33
  K_AND,        // 34
  K_OR,         // 35 - new keyword should be added below this line
  
  K_BACKUP,     // 36
  K_RESTORE,    // 37
  K_ROLLFORWARD,// 38
  K_WITHOUT,    // 39
  K_RF,         // 40
  K_GROUP,		// 41
  
  F_SUM,	    // 42
  F_AVG,        // 43
  F_COUNT,      // 44 - new function name should be added below this line

  S_LEFT_PAREN = 70,  // 70
  S_RIGHT_PAREN,	  // 71
  S_COMMA,			  // 72
  S_STAR,             // 73
  S_EQUAL,            // 74
  S_LESS,             // 75
  S_GREATER,          // 76
	
  ROLLFORWARD_PENDING = 83, // 83
  REDO_IN_PROCESS = 84,		// 84
  IDENT = 85,			    // 85
  INT_LITERAL = 90,			// 90
  STRING_LITERAL,			// 91
  EOC = 95,					// 95
  INVALID = 99				// 99
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 35

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
char *keyword_table[] = 
{
  "int", "char", "create", "table", "not", "null", "drop", "list", "schema",
  "for", "to", "insert", "into", "values", "delete", "from", "where", 
  "update", "set", "select", "order", "by", "desc", "is", "and", "or",
  "backup", "restore", "rollforward", "without", "rf", "group",
  "sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
  INVALID_STATEMENT = -199,	// -199
	CREATE_TABLE = 100,		// 100
	DROP_TABLE,				// 101
	LIST_TABLE,				// 102
	LIST_SCHEMA,			// 103
  INSERT,                   // 104
  DELETE,                   // 105
  UPDATE,                   // 106
  SELECT,                   // 107
  BACKUP,					// 108
  RESTORE,					// 109
  ROLLFORWARD,				// 110
  TIMESTAMP					// 111
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME,		// -398
	TABLE_NOT_EXIST,			// -397
	INVALID_TABLE_DEFINITION,	// -396
	INVALID_COLUMN_NAME,		// -395
	DUPLICATE_COLUMN_NAME,		// -394
	COLUMN_NOT_EXIST,			// -393
	MAX_COLUMN_EXCEEDED,		// -392
	INVALID_TYPE_NAME,			// -391
	INVALID_COLUMN_DEFINITION,	// -390
	INVALID_COLUMN_LENGTH,		// -389
	INVALID_REPORT_FILE_NAME,	// -388
  /* Must add all the possible errors from I/U/D + SELECT here */
	INVALID_RECORD_STRUCT,		// -387
	INVALID_COL_TYPE,			// -386
	COL_NOT_NULL,				// -385
	OVERSIZE_DATA,				// -384
	MAX_RECORD_EXCEEDED,		// -383
	IMAGE_ALREADY_EXIST,		// -382
	RF_START_MISSING,			// -381
	BACKUP_TAG_MISSING,			// -380
	INVALID_TIMESTAMP,			// -379
	LOGFILE_CORRUPTION,			// -378
	IMAGE_NOT_EXIST,			// -377

	FILE_OPEN_ERROR = -299,		// -299
	DBFILE_CORRUPTION,			// -298
	MEMORY_ERROR				// -297
} return_codes;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list, char *query);

int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int sem_insert(token_list *t_list);
int sem_select(token_list *t_list);
int sem_delete(token_list *t_list);
int sem_update(token_list *t_list);

/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);

int create_table_dat(tpd_entry *tpd, int record_size);
int init_tab_header(char *tabname, table_file_header **tab_header);
int insert_table_dat(table_file_header *tab_header, rd_entry *new_record);
int select_table_dat(t_value agg_func, cd_entry *agg_col, table_file_header *tab_header, token_list *col_tok_list[MAX_NUM_COL + 1],
	cd_entry *cst_col_1, t_value cst_op_1, token_list *cst_value_1, t_value cst_join_cond,
	cd_entry *cst_col_2, t_value cst_op_2, token_list *cst_value_2,
	cd_entry *order_col, bool order_asc, cd_entry *group_col);
int select_2table_dat(table_file_header *tab_header, table_file_header *tab_header_2nd,
	token_list *col_tok_list[MAX_NUM_COL + 1],
	cd_entry *cst_col_1, t_value cst_op_1, token_list *cst_value_1, cd_entry *cst_col_1_2nd, t_value cst_join_cond,
	cd_entry *cst_col_2, t_value cst_op_2, token_list *cst_value_2, cd_entry *cst_col_2_2nd,
	cd_entry *order_col, bool order_asc);
int select_3table_dat(table_file_header *tab_header, table_file_header *tab_header_2nd, table_file_header *tab_header_3rd,
	token_list *col_tok_list[MAX_NUM_COL + 1],
	cd_entry *cst_col_1, t_value cst_op_1, token_list *cst_value_1, cd_entry *cst_col_1_2nd, t_value cst_join_cond,
	cd_entry *cst_col_2, t_value cst_op_2, token_list *cst_value_2, cd_entry *cst_col_2_2nd,
	cd_entry *order_col, bool order_asc);
rd_entry* get_cur_record_by_tab(table_file_header *target,
	table_file_header *t1, table_file_header *t2, table_file_header *t3,
	rd_entry *r1, rd_entry *r2, rd_entry *r3);
int init_col_tok_list(token_list *col_entry_list[MAX_NUM_COL + 1], token_list **t_list);
int init_col_len_list(cd_entry *col_entry_list[MAX_NUM_COL + 1], int* col_len_list);
int get_col_display_len(cd_entry *col_entry);
int delete_table_dat(table_file_header *tab_header, cd_entry *cst_col, t_value cst_op, token_list *cst_value);
cd_entry* get_tab_col(tpd_entry *tab_entry, tpd_entry *tab_entry_2nd, tpd_entry *tab_entry_3rd, char* col_name);
table_file_header* get_tab_by_col(cd_entry *col, table_file_header *tab_header, table_file_header *tab_header_2nd, table_file_header *tab_header_3rd);
int verify_col_value(cd_entry *col_entry, token_list *t_list, bool set_value);
int update_table_dat(table_file_header *tab_header, cd_entry *col_entry, token_list* col_value, 
	cd_entry *cst_col, t_value cst_op, token_list *cst_value);

int get_rd_offset_from_record(tpd_entry *tpd, cd_entry *cst_col);
bool verify_constraiant(rd_entry *cst_rd_offset, cd_entry *cst_col, t_value cst_op, token_list *cst_value);
bool verify_join_constraiant(rd_entry *cst_rd_offset, cd_entry *cst_col, rd_entry *cst_rd_offset_2nd, cd_entry *cst_col_2nd);

int sem_backup(token_list *t_list);
int backup_dat(char *filename);

int sem_restore(token_list *t_list);
int restore_dat(char *filename, bool rollforward);
int set_rf_start_flag(char *filename, bool flag);
int prune_log_after_backup(char *filename);
int prune_log_after_timestamp(char *timestamp);

int sem_rollforward(token_list *t_list);
int rollforward_dat(char *timestamp);
int verify_db_flags();
int add_log(int cmd, char *content);
int get_log_offset(char *tag, int *log_num_entry, bool integrity_check);
int get_log_timestamp_offset(char *timestamp);
void create_log_copy(char *log_filename);
int get_log_entry_type(char *log_entry);

int execute_statement(char *statement);
int set_db_flag(int state);