/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list,argv[1]);
		}
		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

  if (g_tpd_list != NULL)
  {
	  free(g_tpd_list);
  }
	getchar();
	
	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
						t_class = function_name;
					else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list, char *query)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_SELECT)
	{
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_DELETE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_UPDATE)
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_BACKUP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TO)))
	{
		printf("BACKUP statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_RESTORE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("RESTORE statement\n");
		cur_cmd = RESTORE;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_ROLLFORWARD)
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		// check for RF_START in log, no checking if recovery is processing
		if (g_tpd_list->db_flags != REDO_IN_PROCESS)
		{
			rc = verify_db_flags();
		}
		if (rc)
		{
			cur->tok_value = INVALID;
		}
		else
		{
			// prevent further query except for rollforward statement
			if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
			{
				if (cur_cmd == ROLLFORWARD)
				{
					rc = sem_rollforward(cur);
				}
				else
				{
					printf("\nROLLFORWARD_PENDING. no further action.");
				}
			}
			// standard query execution
			else
			{
				if (cur_cmd == ROLLFORWARD)
				{
					printf("\nError - database is not in ROLLFORWARD_PENDING state");
					rc = RF_START_MISSING;
					cur->tok_value = INVALID;
				}
				else
				{
					switch (cur_cmd)
					{
					case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
					case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
					case LIST_TABLE:
						rc = sem_list_tables();
						break;
					case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
					case INSERT:
						rc = sem_insert(cur);
						break;
					case SELECT:
						rc = sem_select(cur);
						break;
					case DELETE:
						rc = sem_delete(cur);
						break;
					case UPDATE:
						rc = sem_update(cur);
						break;
					case BACKUP:
						rc = sem_backup(cur);
						break;
					case RESTORE:
						rc = sem_restore(cur);
						break;
					default:
						; /* no action */
					}

					// log the query with timestamp
					if (!g_tpd_list->db_flags 
						&& !rc && cur_cmd != SELECT && cur_cmd != BACKUP 
						&& cur_cmd != RESTORE && cur_cmd != ROLLFORWARD
						&& cur_cmd != LIST_TABLE && cur_cmd != LIST_SCHEMA)
					{
						rc = add_log(cur_cmd, query);
						if (rc)
						{
							cur->tok_value = INVALID;
						}
					}
				}
			}
		}	
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						if (!rc)
						{
							int i, record_size = 0;
							for (i = 0; i < tab_entry.num_columns; i++)
							{	
								record_size += (col_entry[i].col_len + 1);
							}
							record_size += record_size % 4 == 0 ? 0 : 4 - record_size % 4;
							create_table_dat(new_entry, record_size);
						}
						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int create_table_dat(tpd_entry *tpd, int record_size)
{
	int rc = 0;
	FILE *fhandle = NULL;
	char tab_file[MAX_IDENT_LEN + 4 + 1];
	table_file_header tfh;

	memset(tab_file, '\0', MAX_IDENT_LEN + 4 + 1);
	strcat(tab_file, tpd->table_name);
	strcat(tab_file, ".tab");

	if ((fhandle = fopen(tab_file, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		memset(&tfh, '\0', sizeof(table_file_header));
		tfh.file_size = sizeof(table_file_header);
		tfh.record_size = record_size;
		tfh.num_records = 0;
		tfh.record_offset = sizeof(table_file_header);
		tfh.tpd_ptr = NULL;
		fwrite(&tfh, tfh.file_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
		printf("%s new size = %d\n", tab_file, tfh.file_size);
	}
	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				char tab_file[MAX_IDENT_LEN + 4 + 1];
				memset(tab_file, '\0', sizeof(MAX_IDENT_LEN + 4 + 1));
				strcat(tab_file, tab_entry->table_name);
				strcat(tab_file, ".tab");
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				if (!rc){
					if (remove(tab_file) == -1)
					{
						rc = FILE_OPEN_ERROR;
						cur->tok_value = INVALID;
					}
					else
					{
						printf("%s REMOVED.\n", tab_file);
					}
				}
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	table_file_header *tab_header = NULL;
	rd_entry *new_record = NULL;
	rd_entry *record_ptr = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name))
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			rc = init_tab_header(tab_entry->table_name, &tab_header);
			if (!rc)
			{
				if (tab_header->num_records >= MAX_NUM_RECORD)
				{
					rc = MAX_RECORD_EXCEEDED;
					cur->tok_value = INVALID;
				}
				else
				{
					new_record = (rd_entry*)calloc(1, tab_header->record_size);
					if (!new_record)
					{
						rc = MEMORY_ERROR;
						cur->tok_value = INVALID;
					}
					else
					{
						memset(new_record, '\0', tab_header->record_size);
						record_ptr = new_record;
						cur = cur->next;
					}
				}
			}
			else
			{
				cur->tok_value = INVALID;
			}
			if (!rc)
			{
				if (cur->tok_value != K_VALUES)
				{
					rc = INVALID_RECORD_STRUCT;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next;
					if (cur->tok_value != S_LEFT_PAREN)
					{
						rc = INVALID_RECORD_STRUCT;
						cur->tok_value = INVALID;
					}
					else
					{
						cur = cur->next;
						col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
						int i = 0;
						do
						{
							rc = verify_col_value(col_entry, cur, true);
							if (rc)
							{
								cur->tok_value = INVALID;
							}
							else
							{
								if (cur->tok_value == K_NULL)
								{
									int tmp = 0;
									memcpy(record_ptr, &tmp, 1);
								}
								else if (col_entry->col_type == T_INT)
								{
									int tmp = sizeof(int);
									memcpy(record_ptr, &tmp, 1);
									tmp = atoi(cur->tok_string);
									memcpy(record_ptr + 1, &tmp, sizeof(int));
								}
								else if (col_entry->col_type == T_CHAR)
								{
									int tok_len = strlen(cur->tok_string);
									memcpy(record_ptr, &tok_len, 1);
									memcpy(record_ptr + 1, cur->tok_string, tok_len);							
								}
								record_ptr += 1 + col_entry->col_len;
								cur = cur->next;
								if ((i < tab_entry->num_columns - 1 && cur->tok_value != S_COMMA) 
									|| (i == tab_entry->num_columns - 1 && cur->tok_value != S_RIGHT_PAREN))
								{
									rc = INVALID_RECORD_STRUCT;
									cur->tok_value = INVALID;
								}
								else
								{
									i++;
									col_entry++;
									cur = cur->next;
								}
							}
						} while (i < tab_entry->num_columns && !rc);

						if (!rc)
						{
							if (cur->tok_value != EOC)
							{
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							}
							else
							{
								rc = insert_table_dat(tab_header, new_record);
								if (!rc)
								{
									cur->tok_value = INVALID;
								}
							}
						}
					}
				}
			}
			if (new_record)
			{
				free(new_record);
				new_record = NULL;
				record_ptr = NULL;
			}
			if (tab_header)
			{
				free(tab_header);
				tab_header = NULL;
			}
		}
	}
	return rc;
}
int init_tab_header(char *tabname, table_file_header **tab_header)
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	tpd_entry *tab_entry = NULL;
	char tab_file[MAX_IDENT_LEN + 4 + 1];

	if ((tab_entry = get_tpd_from_list(tabname)) == NULL)
	{
		rc = TABLE_NOT_EXIST;
	}
	else
	{
		memset(tab_file, '\0', MAX_IDENT_LEN + 4 + 1);
		strcat(tab_file, tabname);
		strcat(tab_file, ".tab");

		if ((fhandle = fopen(tab_file, "rbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			_fstat(_fileno(fhandle), &file_stat);
			*tab_header = (table_file_header*)calloc(1, file_stat.st_size);

			if (!*tab_header)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				fread(*tab_header, file_stat.st_size, 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
				if ((*tab_header)->file_size != file_stat.st_size)
				{
					rc = DBFILE_CORRUPTION;
				}
				else
				{
					// init tpd_ptr
					(*tab_header)->tpd_ptr = tab_entry;
				}
			}
		}
	}

	return rc;
}
int insert_table_dat(table_file_header *tab_header, rd_entry *new_record)
{
	int rc = 0;
	int old_size = tab_header->file_size;
	FILE *fhandle = NULL;
	char tab_file[MAX_IDENT_LEN + 4 + 1];

	memset(tab_file, '\0', MAX_IDENT_LEN + 4 + 1);
	strcat(tab_file, tab_header->tpd_ptr->table_name);
	strcat(tab_file, ".tab");
	
	if ((fhandle = fopen(tab_file, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		tab_header->file_size += tab_header->record_size;
		tab_header->num_records++;
		tab_header->tpd_ptr = NULL;
		table_file_header *new_tab_header = NULL;
		new_tab_header = (table_file_header *)calloc(1, tab_header->file_size);
		memset(new_tab_header, '\0', tab_header->file_size);
		memcpy(new_tab_header, tab_header, old_size);
		memcpy((void *)((char *)new_tab_header + old_size), (void *)new_record, tab_header->record_size);
		fwrite(new_tab_header, new_tab_header->file_size, 1, fhandle);

		fflush(fhandle);
		fclose(fhandle);
		printf("dbfile.bin size = %d\n", g_tpd_list->list_size);
		printf("INSERT statement\n");
		printf("%s size = %d\n", tab_file, old_size);
		printf("%s new size = %d\n", tab_file, tab_header->file_size);

		if (new_tab_header)
		{
			free(new_tab_header);
			new_tab_header = NULL;
		}
	}

	return rc;
}

int sem_select(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	tpd_entry *tab_entry_2nd = NULL;
	tpd_entry *tab_entry_3rd = NULL;
	table_file_header *tab_header = NULL;
	table_file_header *tab_header_2nd = NULL;
	table_file_header *tab_header_3rd = NULL;
	token_list *col_tok_list[MAX_NUM_COL + 1];
	cur = t_list;

	token_list *agg_col_name = NULL;
	cd_entry *agg_col = NULL;
	t_value agg_func = INVALID;

	token_list *group_identifier = NULL;

	// check for group by aggregate func
	if (cur->tok_value != EOC && cur->next->tok_value !=EOC && cur->next->next->tok_value !=EOC 
		&& (cur->next->next->tok_value == F_SUM || cur->next->next->tok_value == F_AVG || cur->next->next->tok_value == F_COUNT))
	{
		if ((cur->tok_class != keyword)
			&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
		{
			rc = INVALID_COL_TYPE;
			cur->tok_value = INVALID;
		}
		else
		{
			group_identifier = cur;
			cur = cur->next->next;
		}
	}

	// check for aggregate
	if (cur->tok_value == F_SUM || cur->tok_value == F_AVG || cur->tok_value == F_COUNT)
	{
		agg_func = (t_value)cur->tok_value;
		cur = cur->next;
		if (cur->tok_value != S_LEFT_PAREN)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if ((cur->tok_value != S_STAR) && (cur->tok_class != keyword)
				&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
			{
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
			}
			else
			{
				agg_col_name = cur;
				cur = cur->next;
				if (cur->tok_value != S_RIGHT_PAREN)
				{
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next;
				}
			}
		}

	}
	// check for col name list
	else
	{
		rc = init_col_tok_list(col_tok_list, &cur);
	}

	if (!rc)
	{
		if (cur->tok_value != K_FROM)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if ((cur->tok_class != keyword)
				&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
			{
				rc = INVALID_TABLE_NAME;
				cur->tok_value = INVALID;
			}
			else
			{
				if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					rc = init_tab_header(tab_entry->table_name, &tab_header);
					if (rc)
					{
						cur->tok_value = INVALID;
					}
					else
					{
						// validate the sum/avg is on int col
						if (agg_func == F_AVG || agg_func == F_SUM)
						{
							if (agg_col_name->tok_value == S_STAR)
							{
								rc = INVALID_COL_TYPE;
								agg_col_name->tok_value = INVALID;
							}
							else
							{
								agg_col = get_tab_col(tab_entry, NULL, NULL, agg_col_name->tok_string);
								if (agg_col == NULL || agg_col->col_type != T_INT)
								{
									rc = INVALID_COL_TYPE;
									agg_col_name->tok_value = INVALID;
								}
							}
						}
						// validate the count
						else if (agg_func == F_COUNT)
						{
							if (agg_col_name->tok_value != S_STAR
								&& (agg_col = get_tab_col(tab_entry, NULL, NULL, agg_col_name->tok_string)) == NULL)
							{
								rc = COLUMN_NOT_EXIST;
								agg_col_name->tok_value = INVALID;
							}
						}


						cur = cur->next;
						// obtain 2nd table name (optional)
						if (cur->tok_value == S_COMMA)
						{
							// no aggregation for multple table
							if (agg_func != INVALID)
							{
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							}
							else
							{
								cur = cur->next;
								if ((cur->tok_class != keyword)
									&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
								{
									rc = INVALID_TABLE_NAME;
									cur->tok_value = INVALID;
								}
								else
								{
									if ((tab_entry_2nd = get_tpd_from_list(cur->tok_string)) == NULL)
									{
										rc = TABLE_NOT_EXIST;
										cur->tok_value = INVALID;
									}
									else
									{
										rc = init_tab_header(tab_entry_2nd->table_name, &tab_header_2nd);
										if (rc)
										{
											cur->tok_value = INVALID;
										}
										else
										{
											if (stricmp(tab_entry_2nd->table_name, tab_entry->table_name) == 0)
											{
												rc = DUPLICATE_TABLE_NAME;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
												// obtain 3rd table name (optional)
												if (cur->tok_value == S_COMMA)
												{
													cur = cur->next;
													if ((cur->tok_class != keyword)
														&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
													{
														rc = INVALID_TABLE_NAME;
														cur->tok_value = INVALID;
													}
													else
													{
														if ((tab_entry_3rd = get_tpd_from_list(cur->tok_string)) == NULL)
														{
															rc = TABLE_NOT_EXIST;
															cur->tok_value = INVALID;
														}
														else
														{
															rc = init_tab_header(tab_entry_3rd->table_name, &tab_header_3rd);
															if (rc)
															{
																cur->tok_value = INVALID;
															}
															else
															{
																if (stricmp(tab_entry_3rd->table_name, tab_entry->table_name) == 0
																	|| stricmp(tab_entry_3rd->table_name, tab_entry_2nd->table_name) == 0)
																{
																	rc = DUPLICATE_TABLE_NAME;
																	cur->tok_value = INVALID;
																}
																else
																{
																	cur = cur->next;
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}

						// check if the element in col list 
						if (agg_func == INVALID)
						{
							int i = 0;
							while (rc == 0 && col_tok_list[i] != NULL)
							{
								if (get_tab_col(tab_entry, tab_entry_2nd, tab_entry_3rd, col_tok_list[i]->tok_string) == NULL)
								{
									rc = COLUMN_NOT_EXIST;
									col_tok_list[i]->tok_value = INVALID;
								}
								i++;
							}
						}

						if (!rc)
						{
							if (cur->tok_value != K_WHERE && cur->tok_value != EOC
								&& cur->tok_value != K_ORDER && cur->tok_value != K_GROUP && cur->tok_value != K_GROUP)
							{
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							}
							else
							{
								cd_entry *cst_col_1 = NULL;
								t_value cst_op_1 = INVALID;
								token_list *cst_value_1 = NULL;
								cd_entry *cst_col_1_2nd = NULL;

								t_value cst_join_cond = INVALID;
								
								cd_entry *cst_col_2 = NULL;
								t_value cst_op_2 = INVALID;
								token_list *cst_value_2 = NULL;
								cd_entry *cst_col_2_2nd = NULL;

								// WHERE
								if (cur->tok_value == K_WHERE)
								{
									cur = cur->next;
									if ((cst_col_1 = get_tab_col(tab_entry, tab_entry_2nd, tab_entry_3rd, cur->tok_string)) == NULL)
									{
										rc = COLUMN_NOT_EXIST;
										cur->tok_value = INVALID;
									}
									else
									{
										cur = cur->next;
										if ((cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER
											&& cur->tok_value != S_LESS && cur->tok_value != K_IS)
											|| (cur->tok_value == K_IS && cur->next->tok_value != K_NOT && cur->next->tok_value != K_NULL)
											|| (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value != K_NULL))
										{
											rc = INVALID_STATEMENT;
											cur->tok_value = INVALID;
										}
										else
										{
											if (cur->next->tok_value == K_NOT)
											{
												cur = cur->next;
											}
											cst_op_1 = (t_value)cur->tok_value;
											cur = cur->next;
											// check if K_NULL have IS/IS NOT as operator
											if (cur->tok_value == K_NULL && cst_op_1 != K_IS && cst_op_1 != K_NOT)
											{
												rc = INVALID_STATEMENT;
												cur->tok_value = INVALID;
											}
											else
											{
												// if it is a col
												if ((cur->tok_class == keyword || cur->tok_class == identifier || cur->tok_class == type_name)
													&& cur->tok_value != K_NULL)
												{
													if (cst_op_1 != S_EQUAL)
													{
														rc = INVALID_STATEMENT;
													}
													else
													{
														if ((cst_col_1_2nd = get_tab_col(tab_entry, tab_entry_2nd, tab_entry_3rd, cur->tok_string)) == NULL)
														{
															rc = COLUMN_NOT_EXIST;
														}
														else
														{
															if (cst_col_1->col_type != cst_col_1_2nd->col_type)
															{
																rc = INVALID_COL_TYPE;
															}
															else
															{
																//
															}
														}
													}
												}
												// if it is a data value
												else
												{
													rc = verify_col_value(cst_col_1, cur, false);
													if (!rc)
													{
														cst_value_1 = cur;
													}
												}

												if (rc)
												{
													cur->tok_value = INVALID;
												}
												else
												{
													cur = cur->next;
													if (cur->tok_value != EOC && cur->tok_value != K_ORDER && cur->tok_value != K_GROUP
														&& cur->tok_value != K_AND && cur->tok_value != K_OR)
													{
														rc = INVALID_STATEMENT;
														cur->tok_value = INVALID;
													}
													else
													{
														if (cur->tok_value == K_AND || cur->tok_value == K_OR)
														{
															cst_join_cond = (t_value)cur->tok_value;
															cur = cur->next;

															if ((cst_col_2 = get_tab_col(tab_entry, tab_entry_2nd, tab_entry_3rd, cur->tok_string)) == NULL)
															{
																rc = COLUMN_NOT_EXIST;
																cur->tok_value = INVALID;
															}
															else
															{
																cur = cur->next;
																if ((cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER
																	&& cur->tok_value != S_LESS && cur->tok_value != K_IS)
																	|| (cur->tok_value == K_IS && cur->next->tok_value != K_NOT && cur->next->tok_value != K_NULL)
																	|| (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value != K_NULL))
																{
																	rc = INVALID_STATEMENT;
																	cur->tok_value = INVALID;
																}
																else
																{
																	if (cur->next->tok_value == K_NOT)
																	{
																		cur = cur->next;
																	}
																	cst_op_2 = (t_value)cur->tok_value;
																	cur = cur->next;
																	// check if K_NULL have IS/IS NOT as operator
																	if (cur->tok_value == K_NULL && cst_op_2 != K_IS && cst_op_2 != K_NOT)
																	{
																		rc = INVALID_STATEMENT;
																		cur->tok_value = INVALID;
																	}
																	else
																	{
																		// if it is a col
																		if ((cur->tok_class == keyword || cur->tok_class == identifier || cur->tok_class == type_name)
																			&& cur->tok_value != K_NULL)
																		{
																			if (cst_op_2 != S_EQUAL)
																			{
																				rc = INVALID_STATEMENT;
																			}
																			else
																			{
																				if ((cst_col_2_2nd = get_tab_col(tab_entry, tab_entry_2nd, tab_entry_3rd, cur->tok_string)) == NULL)
																				{
																					rc = COLUMN_NOT_EXIST;
																				}
																				else
																				{
																					if (cst_col_2->col_type !=  cst_col_2_2nd->col_type)
																					{
																						rc = INVALID_COL_TYPE;
																					}
																					else
																					{
																						//
																					}
																				}
																			}
																		}
																		// if it is a data value
																		else
																		{
																			rc = verify_col_value(cst_col_2, cur, false);
																			if (!rc)
																			{
																				cst_value_2 = cur;
																			}
																		}

																		if (rc)
																		{
																			cur->tok_value = INVALID;
																		}
																		else
																		{
																			cur = cur->next;
																			if (cur->tok_value != EOC && cur->tok_value != K_ORDER && cur->tok_value != K_GROUP)
																			{
																				rc = INVALID_STATEMENT;
																				cur->tok_value = INVALID;
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}

								// ORDER BY
								cd_entry *order_col = NULL;
								bool order_asc = true;
								if (cur->tok_value == K_ORDER)
								{
									cur = cur->next;
									if (cur->tok_value != K_BY)
									{
										rc = INVALID_STATEMENT;
										cur->tok_value = INVALID;
									}
									else
									{
										cur = cur->next;
										if ((cur->tok_class != keyword)
											&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
										{
											rc = INVALID_COLUMN_NAME;
											cur->tok_value = INVALID;
										}
										else
										{
											if ((order_col = get_tab_col(tab_entry, tab_entry_2nd, tab_entry_3rd, cur->tok_string)) == NULL)
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
												if (cur->tok_value != EOC && cur->tok_value != K_DESC && cur->tok_value != K_GROUP)
												{
													rc = INVALID_STATEMENT;
													cur->tok_value = INVALID;
												}
												else
												{
													if (cur->tok_value == K_DESC)
													{
														order_asc = false;
														cur = cur->next;
														if (cur->tok_value != EOC)
														{
															rc = INVALID_STATEMENT;
															cur->tok_value = INVALID;
														}
													}
												}
											}
										}
									}
								}

								// GROUP BY
								cd_entry *group_col = NULL;
								if (cur->tok_value == K_GROUP)
								{
									// group by must be follow agg func
									if (agg_func == INVALID || order_col != NULL)
									{
										rc = INVALID_STATEMENT;
										cur->tok_value = INVALID;
									}
									else
									{
										cur = cur->next;
										if (cur->tok_value != K_BY)
										{
											rc = INVALID_STATEMENT;
											cur->tok_value = INVALID;
										}
										else
										{
											cur = cur->next;
											if ((cur->tok_class != keyword && cur->tok_class != identifier && cur->tok_class != type_name)
												|| (group_identifier != NULL && stricmp(group_identifier->tok_string, cur->tok_string) != 0))
											{
												rc = INVALID_COLUMN_NAME;
												cur->tok_value = INVALID;
											}
											else
											{
												if ((group_col = get_tab_col(tab_header->tpd_ptr, NULL, NULL, cur->tok_string)) == NULL)
												{
													rc = COLUMN_NOT_EXIST;
													cur->tok_value = INVALID;
												}
												else
												{
													//
												}
											}
										}
									}
								}
								// if there is a grouping identifier but no group by clause, prompt error
								if (group_identifier != NULL && group_col == NULL)
								{
									rc = INVALID_STATEMENT;
									group_identifier->tok_value = INVALID;
								}

								// END
								if (rc)
								{
									cur->tok_value = INVALID;
								}
								else
								{
									// if there is only one table
									if (tab_entry_2nd == NULL)
									{
										rc = select_table_dat(agg_func, agg_col,
											tab_header, col_tok_list,
											cst_col_1, cst_op_1, cst_value_1, cst_join_cond,
											cst_col_2, cst_op_2, cst_value_2,
											order_col, order_asc, group_col);
									}
									// if there is two table
									else if (tab_entry_3rd == NULL)
									{
										rc = select_2table_dat(tab_header, tab_header_2nd, col_tok_list,
											cst_col_1, cst_op_1, cst_value_1, cst_col_1_2nd, cst_join_cond,
											cst_col_2, cst_op_2, cst_value_2, cst_col_2_2nd,
											order_col, order_asc);
									}
									// if there is three table
									else
									{
										rc = select_3table_dat(tab_header, tab_header_2nd, tab_header_3rd, col_tok_list,
											cst_col_1, cst_op_1, cst_value_1, cst_col_1_2nd, cst_join_cond,
											cst_col_2, cst_op_2, cst_value_2, cst_col_2_2nd,
											order_col, order_asc);
									}
									if (rc)
									{
										cur->tok_value = INVALID;
									}
								}
							}
						}
					}
				}
			}
		}
	}
	if (tab_header)
	{
		free(tab_header);
		tab_header = NULL;
	}
	if (tab_header_2nd)
	{
		free(tab_header_2nd);
		tab_header_2nd = NULL;
	}
	if (tab_header_3rd)
	{
		free(tab_header_3rd);
		tab_header_3rd = NULL;
	}
	return rc;
}

int init_col_tok_list(token_list *col_tok_list[MAX_NUM_COL + 1], token_list **t_list)
{
	// null col list with rc = 0 stands for * (all column)
	int rc = 0;
	token_list *cur = *t_list;
	bool star_flag = false;
	
	int i;
	for (i = 0; i < MAX_NUM_COL + 1; i++)
	{
		col_tok_list[i] = NULL;
	}
	// skip the rest if only one star in the list
	if (cur->tok_value == S_STAR && cur->next->tok_value != S_COMMA)
	{
		star_flag = true;
		cur= cur->next;
	}
	// verify a list of col name
	i = 0;
	if (!star_flag)
	do
	{
		if (i >= MAX_NUM_COL)
		{
			rc = MAX_COLUMN_EXCEEDED;
			cur->tok_value = INVALID;
		}
		else
		{
			if (cur->tok_class != keyword
				&& cur->tok_class != identifier
				&& cur->tok_class != type_name)
			{
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
			}
			else
			{
				int j;
				bool found = false;
				for (j = 0; j < i; j++)
				{
					if (stricmp(col_tok_list[j]->tok_string, cur->tok_string) == 0)
					{
						found = true;
					}
				}
				if (found)
				{
					rc = DUPLICATE_COLUMN_NAME;
					cur->tok_value = INVALID;
				}
				else
				{
					col_tok_list[i] = cur;
					cur = cur->next;
					if (cur->tok_value != S_COMMA)
					{
						break;
					}
					else
					{
						i++;
						cur = cur->next;
					}
				}
			}
		}
	} while (rc == 0);
	*t_list = cur;
	
	return rc;
}

int select_table_dat(t_value agg_func, cd_entry *agg_col, table_file_header *tab_header, token_list *col_tok_list[MAX_NUM_COL + 1],
	cd_entry *cst_col_1, t_value cst_op_1, token_list *cst_value_1, t_value cst_join_cond, 
	cd_entry *cst_col_2, t_value cst_op_2, token_list *cst_value_2,
	cd_entry *order_col, bool order_asc, cd_entry *group_col)
{
	int rc = 0;
	int num_col = 0;
	int num_selected = 0;
	int sum = 0;
	cd_entry *col_entry;
	tpd_entry *tab_entry = tab_header->tpd_ptr;
	rd_entry *record_ptr;
	int col_len_list[MAX_NUM_COL];
	cd_entry *col_entry_list[MAX_NUM_COL+1];

	// print file details
	printf("dbfile.bin size = %d\n", g_tpd_list->list_size);
	printf("SELECT statement\n");
	printf("%s.tab size = %d\n", tab_header->tpd_ptr->table_name, tab_header->file_size);

	// if grouping exist, then order it by the group and ignore the existing order by clauses
	if (group_col != NULL)
	{
		order_col = group_col;
	}
	// order the table by col if specified
	if (order_col != NULL)
	{
		rd_entry *extreme_record = (rd_entry*)calloc(1, tab_header->record_size);
		int order_offset = get_rd_offset_from_record(tab_header->tpd_ptr, order_col);
		int r;
		for (r = 0; r < tab_header->num_records; r++)
		{
			// get the extreme after rth position value and swap to the current r postion
			int extreme_idx = r;
			int i;
			for (i = r + 1, record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset + (r + 1) * tab_header->record_size);
				i < tab_header->num_records; i++, record_ptr += tab_header->record_size)
			{
				rd_entry *record_offset = (rd_entry *)((char*)record_ptr + order_offset);
				rd_entry *extreme_offset = (rd_entry *)((char *)tab_header + tab_header->record_offset + extreme_idx * tab_header->record_size + order_offset);

				if (order_col->col_type == T_INT)
				{
					if (order_asc && *((int*)(record_offset + 1)) < *((int*)(extreme_offset + 1)))
					{
						extreme_idx = i;
					}
					else if (!order_asc && *((int*)(record_offset + 1)) > *((int*)(extreme_offset + 1)))
					{
						extreme_idx = i;
					}
				}
				else if (order_col->col_type == T_CHAR)
				{
					char tmp[MAX_CHAR_LEN + 1];
					memset(tmp, '\0', MAX_CHAR_LEN + 1);
					memcpy(tmp, record_offset + 1, (int)*((char *)record_offset));

					char extreme_tmp[MAX_CHAR_LEN + 1];
					memset(extreme_tmp, '\0', MAX_CHAR_LEN + 1);
					memcpy(extreme_tmp, extreme_offset + 1, (int)*((char *)extreme_offset));
					if (order_asc && strcmp(tmp, extreme_tmp) < 0)
					{
						extreme_idx = i;
					}
					else if (!order_asc && strcmp(tmp, extreme_tmp) > 0)
					{
						extreme_idx = i;
					}
				}
			}
			// store the min/max record to tmp memory
			memcpy(extreme_record, 
				(rd_entry *)((char *)tab_header + tab_header->record_offset + extreme_idx * tab_header->record_size), 
				tab_header->record_size);
			// shift all the records by one position			
			for (i = extreme_idx; i > r; i--)
			{
				rd_entry *cur_record = (rd_entry *)((char *)tab_header + tab_header->record_offset + i * tab_header->record_size);
				rd_entry *prv_record = (rd_entry *)((char *)tab_header + tab_header->record_offset + (i - 1) * tab_header->record_size);
				memcpy(cur_record, prv_record, tab_header->record_size);
			}
			// set the current rth record to the min/max record
			memcpy((rd_entry *)((char *)tab_header + tab_header->record_offset + r * tab_header->record_size),
				extreme_record,
				tab_header->record_size);
		}
		if (extreme_record != NULL)
		{
			free(extreme_record);
		}
	}

	// select with aggregation function
	if (agg_func == F_AVG || agg_func == F_SUM || agg_func == F_COUNT)
	{
		// print heading
		printf("\n");
		int group_col_len;
		if (group_col != NULL)
		{
			group_col_len = get_col_display_len(group_col);
			printf(" %-*s |", group_col_len, group_col->col_name);
		}
		if (agg_func == F_AVG)
		{
			printf(" avg(%s)\n", agg_col->col_name);
			int i;
			for (i = 0; i < strlen(agg_col->col_name) + 5 + 4; i++)
			{
				printf("-");
			}
		}
		else if (agg_func == F_SUM)
		{
			printf(" sum(%s)\n", agg_col->col_name);
			int i;
			for (i = 0; i < strlen(agg_col->col_name) + 5 + 4; i++)
			{
				printf("-");
			}
		}
		else if (agg_func == F_COUNT)
		{
			printf(" count(%s)\n", agg_col == NULL ? "*" : agg_col->col_name);
			int i;
			for (i = 0; i < strlen(agg_col == NULL ? "*" : agg_col->col_name) + 7 + 4; i++)
			{
				printf("-");
			}
		}
		int i;
		if (group_col != NULL)
		{
			for (i = 0; i < 1 + group_col_len + 1; i++)
			{
				printf("-");
			}
		}
		printf("\n");

		// check for condition
		int cst_offset_1;
		int cst_offset_2;
		if (cst_col_1 != NULL)
		{
			cst_offset_1 = get_rd_offset_from_record(tab_header->tpd_ptr, cst_col_1);
			if (cst_col_2 != NULL)
			{
				cst_offset_2 = get_rd_offset_from_record(tab_header->tpd_ptr, cst_col_2);
			}
		}

		// init current "group by" value pointer
		int group_offset;
		if (group_col != NULL)
		{
			group_offset = get_rd_offset_from_record(tab_header->tpd_ptr, group_col);
		}
		rd_entry *record_group_value = NULL;

		for (i = 0, record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset);
			i < tab_header->num_records; i++, record_ptr += tab_header->record_size)
		{
			bool valid1, valid2, valid;

			// if there is a constraiant
			if (cst_col_1 != NULL)
			{
				valid1 = verify_constraiant((rd_entry *)((char*)record_ptr + cst_offset_1), cst_col_1, cst_op_1, cst_value_1);
				if (cst_col_2 != NULL)
				{
					valid2 = verify_constraiant((rd_entry *)((char*)record_ptr + cst_offset_2), cst_col_2, cst_op_2, cst_value_2);
				}
			}
			if (cst_join_cond == K_AND)
			{
				// 2 constraint with AND cond
				valid = valid1 && valid2;
			}
			else if (cst_join_cond == K_OR)
			{
				// 2 constraint with OR cond
				valid = valid1 || valid2;
			}
			else if (cst_col_1 != NULL)
			{
				// only 1 constraint, so depends on valid = valid1
				valid = valid1;
			}
			else
			{
				// 0 constraint, valid = true
				valid = true;
			}

			if (valid)
			{
				// check for each group
				rd_entry *new_group_value = (rd_entry *)((char*)record_ptr + group_offset);
				if (record_group_value == NULL)
				{
					record_group_value = new_group_value;
				}
				else
				{
					// if the group value is not equal, print the summary and step to the next group
					if (group_col != NULL
						&& strncmp((char*)record_group_value + 1, (char*)new_group_value + 1, group_col->col_len) != 0)
					{
						if ((int)*((char *)record_group_value) == 0)
						{
							printf(" %-*s |", group_col_len, "-");
						}
						else
						{
							if (group_col->col_type == T_INT)
							{
								printf(" %-*d |", group_col_len, *((int*)((char*)record_group_value + 1)));
							}
							else if (group_col->col_type == T_CHAR)
							{
								char tmp[MAX_COL_DISPLAY_LEN + 1];
								memset(tmp, '\0', MAX_COL_DISPLAY_LEN + 1);
								memcpy(tmp, record_group_value + 1, (int)*((char *)record_group_value) > group_col_len
									? group_col_len : (int)*((char *)record_group_value));
								printf(" %-*s |", group_col_len, tmp);

							}
						}
						if (num_selected == 0)
						{
							printf(" -");
						}
						else
						{
							if (agg_func == F_AVG)
							{
								printf(" %.4f", (float)sum / num_selected);
							}
							else if (agg_func == F_SUM)
							{
								printf(" %d", sum);
							}
							else if (agg_func == F_COUNT)
							{
								printf(" %d", num_selected);
							}
						}
						printf("\n");
						sum = 0;
						num_selected = 0;
						record_group_value = new_group_value;
					}
				}

				if (agg_func == F_AVG || agg_func == F_SUM)
				{
					int agg_rd_offset = get_rd_offset_from_record(tab_header->tpd_ptr, agg_col);
					rd_entry *agg_rd_entry = (rd_entry *)((char*)record_ptr + agg_rd_offset);
					if ((int)*((char *)agg_rd_entry) > 0)
					{
						sum += *((int*)(agg_rd_entry + 1));
						num_selected++;
					}
				}
				else if (agg_func == F_COUNT)
				{
					// all col *
					if (agg_col == NULL)
					{
						num_selected++;
					}
					// any specific col
					else
					{
						int agg_rd_offset = get_rd_offset_from_record(tab_header->tpd_ptr, agg_col);
						rd_entry *agg_rd_entry = (rd_entry *)((char*)record_ptr + agg_rd_offset);
						if ((int)*((char *)agg_rd_entry) > 0)
						{
							num_selected++;
						}
					}
				}
			}
		}
		// print leftover data
		if (group_col != NULL)
		{
			if ((int)*((char *)record_group_value) == 0)
			{
				printf(" %-*s |", group_col_len, "-");
			}
			else
			{
				if (group_col->col_type == T_INT)
				{
					printf(" %-*d |", group_col_len, *((int*)((char*)record_group_value + 1)));
				}
				else if (group_col->col_type == T_CHAR)
				{
					char tmp[MAX_COL_DISPLAY_LEN + 1];
					memset(tmp, '\0', MAX_COL_DISPLAY_LEN + 1);
					memcpy(tmp, record_group_value + 1, (int)*((char *)record_group_value) > group_col_len
						? group_col_len : (int)*((char *)record_group_value));
					printf(" %-*s |", group_col_len, tmp);
				}
			}
		}
		if (num_selected == 0)
		{
			printf(" -");
		}
		else
		{
			if (agg_func == F_AVG)
			{
				printf(" %.4f", (float)sum / num_selected);
			}
			else if (agg_func == F_SUM)
			{
				printf(" %d", sum);
			}
			else if (agg_func == F_COUNT)
			{
				printf(" %d", num_selected);
			}
		}
		printf("\n");
	}
	// select without aggregation function
	else
	{
		// get a col list
		int i = 0;
		for (i = 0; i < MAX_NUM_COL + 1; i++)
		{
			if (col_tok_list[i] != NULL)
			{
				col_entry_list[i] = get_tab_col(tab_header->tpd_ptr, NULL, NULL, col_tok_list[i]->tok_string);
				num_col = i;
			}
			else
			{
				col_entry_list[i] = NULL;
			}
		}
		num_col++;
		// get a complete list if not specified
		i = 0;
		if (col_entry_list[0] == NULL)
		{
			cd_entry* col_entry_ptr;
			for (i = 0, col_entry_ptr = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				i < tab_entry->num_columns; i++, col_entry_ptr++)
			{
				col_entry_list[i] = col_entry_ptr;
			}
			num_col = i;
		}
		// pre-computed the len of each col
		init_col_len_list(col_entry_list, col_len_list);
		printf("\n");
		// print record header
		for (i = 0; i < num_col; i++)
		{
			if (i != 0)
			{
				printf(" ");
			}
			printf("%-*s", col_len_list[i], col_entry_list[i]->col_name);
			printf(" ");
			if (i != num_col - 1)
			{
				printf("|");
			}
		}
		printf("\n");
		// print seperator
		for (i = 0; i < num_col; i++)
		{
			int j;
			for (j = 0; j < col_len_list[i]; j++)
			{
				printf("-");
			}
			// space after a col value
			printf("-");
		}
		// ignore the first space the last seperator
		for (i = 0; i < num_col - 1; i++)
		{
			printf("--");
		}
		printf("\n");

		// print record
		int cst_offset_1;
		int cst_offset_2;
		if (cst_col_1 != NULL)
		{
			cst_offset_1 = get_rd_offset_from_record(tab_header->tpd_ptr, cst_col_1);
			if (cst_col_2 != NULL)
			{
				cst_offset_2 = get_rd_offset_from_record(tab_header->tpd_ptr, cst_col_2);
			}
		}
		for (i = 0, record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset);
			i < tab_header->num_records; i++, record_ptr += tab_header->record_size)
		{
			bool valid1, valid2, valid;

			// if there is a constraiant
			if (cst_col_1 != NULL)
			{
				valid1 = verify_constraiant((rd_entry *)((char*)record_ptr + cst_offset_1), cst_col_1, cst_op_1, cst_value_1);
				if (cst_col_2 != NULL)
				{
					valid2 = verify_constraiant((rd_entry *)((char*)record_ptr + cst_offset_2), cst_col_2, cst_op_2, cst_value_2);
				}
			}
			if (cst_join_cond == K_AND)
			{
				// 2 constraint with AND cond
				valid = valid1 && valid2;
			}
			else if (cst_join_cond == K_OR)
			{
				// 2 constraint with OR cond
				valid = valid1 || valid2;
			}
			else if (cst_col_1 != NULL)
			{
				// only 1 constraint, so depends on valid = valid1
				valid = valid1;
			}
			else
			{
				// 0 constraint, valid = true
				valid = true;
			}

			// reset the col value to '\0'
			if (valid)
			{
				num_selected++;
				int j;
				for (j = 0; j < num_col; j++)
				{
					rd_entry *record_offset = record_ptr;
					if (j != 0)
					{
						printf(" ");
					}
					int col_value_offset = 0;
					int k;
					int col_idx = col_entry_list[j] -
						(cd_entry *)((char *)tab_header->tpd_ptr + tab_header->tpd_ptr->cd_offset);
					for (k = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
						k < col_idx; k++, col_entry++)
					{
						col_value_offset += 1 + col_entry->col_len;

					}
					record_offset = (rd_entry *)((char*)record_offset + col_value_offset);
					if ((int)*((char *)record_offset) == 0)
					{
						if (col_entry_list[j]->col_type == T_INT)
						{
							printf("%*s", col_len_list[j], "-");
						}
						else if (col_entry_list[j]->col_type == T_CHAR)
						{
							printf("%-*s", col_len_list[j], "-");
						}
					}
					else
					{
						if (col_entry_list[j]->col_type == T_INT)
						{
							printf("%*d", col_len_list[j], *((int*)(record_offset + 1)));
						}
						else if (col_entry_list[j]->col_type == T_CHAR)
						{
							char tmp[MAX_COL_DISPLAY_LEN + 1];
							memset(tmp, '\0', MAX_COL_DISPLAY_LEN + 1);
							memcpy(tmp, record_offset + 1, (int)*((char *)record_offset) > col_len_list[j]
								? col_len_list[j] : (int)*((char *)record_offset));
							printf("%-*s", col_len_list[j], tmp);
						}
					}
					printf(" ");
					if (j != num_col - 1)
					{
						printf("|");
					}
				}
				printf("\n");
			}
		}
		printf("\n");
		printf("%d rows selected.", num_selected);
	}

	return rc;
}

int select_2table_dat(table_file_header *tab_header, table_file_header *tab_header_2nd,
	token_list *col_tok_list[MAX_NUM_COL + 1],
	cd_entry *cst_col_1, t_value cst_op_1, token_list *cst_value_1, cd_entry *cst_col_1_2nd, t_value cst_join_cond,
	cd_entry *cst_col_2, t_value cst_op_2, token_list *cst_value_2, cd_entry *cst_col_2_2nd,
	cd_entry *order_col, bool order_asc)
{
	int rc = 0;
	int num_col = 0;
	int num_selected = 0;
	int sum = 0;
	cd_entry *col_entry;
	tpd_entry *tab_entry = tab_header->tpd_ptr;
	tpd_entry *tab_entry_2nd = tab_header_2nd->tpd_ptr;
	rd_entry *record_ptr, *record_ptr_2nd;
	int col_len_list[MAX_NUM_COL];
	cd_entry *col_entry_list[MAX_NUM_COL + 1];

	// print file details
	printf("dbfile.bin size = %d\n", g_tpd_list->list_size);
	printf("SELECT statement\n");
	printf("%s.tab size = %d\n", tab_header->tpd_ptr->table_name, tab_header->file_size);

	/////////////////////////////////////////////////////////////////////////////////////
	//////// PRINT HEADING
	/////////////////////////////////////////////////////////////////////////////////////
	// get a col list
	int i = 0;
	for (i = 0; i < MAX_NUM_COL + 1; i++)
	{
		if (col_tok_list[i] != NULL)
		{
			col_entry_list[i] = get_tab_col(tab_entry, tab_entry_2nd, NULL, col_tok_list[i]->tok_string);
			num_col = i;
		}
		else
		{
			col_entry_list[i] = NULL;
		}
	}
	num_col++;
	// get a complete list if not specified
	i = 0;
	if (col_entry_list[0] == NULL)
	{
		cd_entry* col_entry_ptr;
		for (i = 0, col_entry_ptr = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			i < tab_entry->num_columns; i++, col_entry_ptr++)
		{
			col_entry_list[i] = col_entry_ptr;
		}
		int j;
		for (j = 0, col_entry_ptr = (cd_entry*)((char*)tab_entry_2nd + tab_entry_2nd->cd_offset);
			j < tab_entry_2nd->num_columns; j++, col_entry_ptr++)
		{
			col_entry_list[i + j] = col_entry_ptr;
		}
		num_col = j + i;
	}
	// pre-computed the len of each col
	init_col_len_list(col_entry_list, col_len_list);
	printf("\n");
	// print record header
	for (i = 0; i < num_col; i++)
	{
		if (i != 0)
		{
			printf(" ");
		}
		printf("%-*s", col_len_list[i], col_entry_list[i]->col_name);
		printf(" ");
		if (i != num_col - 1)
		{
			printf("|");
		}
	}
	printf("\n");
	// print seperator
	for (i = 0; i < num_col; i++)
	{
		int j;
		for (j = 0; j < col_len_list[i]; j++)
		{
			printf("-");
		}
		// space after a col value
		printf("-");
	}
	// ignore the first space the last seperator
	for (i = 0; i < num_col - 1; i++)
	{
		printf("--");
	}
	printf("\n");

	/////////////////////////////////////////////////////////////////////////////////////
	//////// ORDER the table
	/////////////////////////////////////////////////////////////////////////////////////
	// order the table by col if specified
	if (order_col != NULL)
	{
		table_file_header *order_tab = get_tab_by_col(order_col, tab_header, tab_header_2nd, NULL);
		rd_entry *extreme_record = (rd_entry*)calloc(1, order_tab->record_size);
		int order_offset = get_rd_offset_from_record(order_tab->tpd_ptr, order_col);
		int r;
		for (r = 0; r < order_tab->num_records; r++)
		{
			// get the extreme after rth position value and swap to the current r postion
			int extreme_idx = r;
			int i;
			for (i = r + 1, record_ptr = (rd_entry *)((char *)order_tab + order_tab->record_offset + (r + 1) * order_tab->record_size);
				i < order_tab->num_records; i++, record_ptr += order_tab->record_size)
			{
				rd_entry *record_offset = (rd_entry *)((char*)record_ptr + order_offset);
				rd_entry *extreme_offset = (rd_entry *)((char *)order_tab + order_tab->record_offset + extreme_idx * order_tab->record_size + order_offset);

				if (order_col->col_type == T_INT)
				{
					if (order_asc && *((int*)(record_offset + 1)) < *((int*)(extreme_offset + 1)))
					{
						extreme_idx = i;
					}
					else if (!order_asc && *((int*)(record_offset + 1)) > *((int*)(extreme_offset + 1)))
					{
						extreme_idx = i;
					}
				}
				else if (order_col->col_type == T_CHAR)
				{
					char tmp[MAX_CHAR_LEN + 1];
					memset(tmp, '\0', MAX_CHAR_LEN + 1);
					memcpy(tmp, record_offset + 1, (int)*((char *)record_offset));

					char extreme_tmp[MAX_CHAR_LEN + 1];
					memset(extreme_tmp, '\0', MAX_CHAR_LEN + 1);
					memcpy(extreme_tmp, extreme_offset + 1, (int)*((char *)extreme_offset));
					if (order_asc && strcmp(tmp, extreme_tmp) < 0)
					{
						extreme_idx = i;
					}
					else if (!order_asc && strcmp(tmp, extreme_tmp) > 0)
					{
						extreme_idx = i;
					}
				}
			}
			// store the min/max record to tmp memory
			memcpy(extreme_record,
				(rd_entry *)((char *)order_tab + order_tab->record_offset + extreme_idx * order_tab->record_size),
				order_tab->record_size);
			// shift all the records by one position			
			for (i = extreme_idx; i > r; i--)
			{
				rd_entry *cur_record = (rd_entry *)((char *)order_tab + order_tab->record_offset + i * order_tab->record_size);
				rd_entry *prv_record = (rd_entry *)((char *)order_tab + order_tab->record_offset + (i - 1) * order_tab->record_size);
				memcpy(cur_record, prv_record, order_tab->record_size);
			}
			// set the current rth record to the min/max record
			memcpy((rd_entry *)((char *)order_tab + order_tab->record_offset + r * order_tab->record_size),
				extreme_record,
				order_tab->record_size);
		}
		if (extreme_record != NULL)
		{
			free(extreme_record);
		}
		// put order tab in outter loop
		if (tab_header_2nd == order_tab)
		{
			tab_header_2nd = tab_header;
			tab_header = order_tab;
			tab_entry = tab_header->tpd_ptr;
			tab_entry_2nd = tab_header_2nd->tpd_ptr;
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////
	//////// NESTED LOOP JOIN and print record
	/////////////////////////////////////////////////////////////////////////////////////
	int cst_offset_1, cst_offset_1_2nd, cst_offset_2, cst_offset_2_2nd;
	table_file_header *cst_tab_1, *cst_tab_1_2nd, *cst_tab_2, *cst_tab_2_2nd;
	// init constraint offsets
	if (cst_col_1 != NULL)
	{
		cst_tab_1 = get_tab_by_col(cst_col_1, tab_header, tab_header_2nd, NULL);
		// if second arg of the first condition is a col then calculate the offset
		cst_tab_1_2nd = cst_col_1_2nd != NULL ? get_tab_by_col(cst_col_1_2nd, tab_header, tab_header_2nd, NULL) : 0;
		cst_offset_1 = get_rd_offset_from_record(cst_tab_1->tpd_ptr, cst_col_1);
		cst_offset_1_2nd = cst_col_1_2nd != NULL ? get_rd_offset_from_record(cst_tab_1_2nd->tpd_ptr, cst_col_1_2nd) : 0;
		if (cst_col_2 != NULL)
		{
			cst_tab_2 = get_tab_by_col(cst_col_2, tab_header, tab_header_2nd, NULL);
			// if second arg of the second condition is a col then calculate the offset
			cst_tab_2_2nd = cst_col_2_2nd != NULL ? get_tab_by_col(cst_col_2_2nd, tab_header, tab_header_2nd, NULL) : 0;
			cst_offset_2 = get_rd_offset_from_record(cst_tab_2->tpd_ptr, cst_col_2);
			cst_offset_2_2nd = cst_col_2_2nd != NULL ? get_rd_offset_from_record(cst_tab_2_2nd->tpd_ptr, cst_col_2_2nd) : 0;
		}
	}
	
	int i_2;
	for (i = 0, record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset);
		i < tab_header->num_records; i++, record_ptr += tab_header->record_size)
	{
		for (i_2 = 0, record_ptr_2nd = (rd_entry *)((char *)tab_header_2nd + tab_header_2nd->record_offset);
			i_2 < tab_header_2nd->num_records; i_2++, record_ptr_2nd += tab_header_2nd->record_size)
		{
			bool valid1, valid2, valid;
			
			// if there is a constraiant
			if (cst_col_1 != NULL)
			{
				// check join condition
				if (cst_col_1_2nd != NULL)
				{
					valid1 = verify_join_constraiant((rd_entry *)((char*)(cst_tab_1 == tab_header ? record_ptr : record_ptr_2nd) + cst_offset_1), cst_col_1, 
						(rd_entry *)((char*)(cst_tab_1_2nd == tab_header ? record_ptr : record_ptr_2nd) + cst_offset_1_2nd), cst_col_1_2nd);
				}
				// check data value condition
				else
				{
					valid1 = verify_constraiant((rd_entry *)((char*)(cst_tab_1 == tab_header ? record_ptr : record_ptr_2nd) + cst_offset_1), cst_col_1, cst_op_1, cst_value_1);
				}
				if (cst_col_2 != NULL)
				{
					// check join condition
					if (cst_col_2_2nd != NULL)
					{
						valid2 = verify_join_constraiant((rd_entry *)((char*)(cst_tab_2 == tab_header ? record_ptr : record_ptr_2nd) + cst_offset_2), cst_col_2,
							(rd_entry *)((char*)(cst_tab_2_2nd == tab_header ? record_ptr : record_ptr_2nd) + cst_offset_2_2nd), cst_col_2);
					}
					// check data value condition
					else
					{
						valid2 = verify_constraiant((rd_entry *)((char*)(cst_tab_2 == tab_header ? record_ptr : record_ptr_2nd) + cst_offset_2), cst_col_2, cst_op_2, cst_value_2);
					}
				}
			}
			if (cst_join_cond == K_AND)
			{
				// 2 constraint with AND cond
				valid = valid1 && valid2;
			}
			else if (cst_join_cond == K_OR)
			{
				// 2 constraint with OR cond
				valid = valid1 || valid2;
			}
			else if (cst_col_1 != NULL)
			{
				// only 1 constraint, so depends on valid = valid1
				valid = valid1;
			}
			else
			{
				// 0 constraint, valid = true
				valid = true;
			}
			
			// reset the col value to '\0'
			if (valid)
			{
				num_selected++;
				int j;
				for (j = 0; j < num_col; j++)
				{
					table_file_header *target_tab_header = get_tab_by_col(col_entry_list[j], tab_header, tab_header_2nd, NULL);
					// if it is the first table
					rd_entry *record_offset;
					if (target_tab_header == tab_header)
					{
						record_offset = record_ptr;
					}
					// if it is sec table
					else if (target_tab_header == tab_header_2nd)
					{
						record_offset = record_ptr_2nd;
					}
					
					if (j != 0)
					{
						printf(" ");
					}
					int col_value_offset = get_rd_offset_from_record(target_tab_header->tpd_ptr, col_entry_list[j]);
					int k;
					record_offset = (rd_entry *)((char*)record_offset + col_value_offset);
					if ((int)*((char *)record_offset) == 0)
					{
						if (col_entry_list[j]->col_type == T_INT)
						{
							printf("%*s", col_len_list[j], "-");
						}
						else if (col_entry_list[j]->col_type == T_CHAR)
						{
							printf("%-*s", col_len_list[j], "-");
						}
					}
					else
					{
						if (col_entry_list[j]->col_type == T_INT)
						{
							printf("%*d", col_len_list[j], *((int*)(record_offset + 1)));
						}
						else if (col_entry_list[j]->col_type == T_CHAR)
						{
							char tmp[MAX_COL_DISPLAY_LEN + 1];
							memset(tmp, '\0', MAX_COL_DISPLAY_LEN + 1);
							memcpy(tmp, record_offset + 1, (int)*((char *)record_offset) > col_len_list[j]
								? col_len_list[j] : (int)*((char *)record_offset));
							printf("%-*s", col_len_list[j], tmp);
						}
					}
					printf(" ");
					if (j != num_col - 1)
					{
						printf("|");
					}
				}
				printf("\n");
			}
		}
	}
	printf("\n");
	printf("%d rows selected.", num_selected);
	
	return rc;
}

int select_3table_dat(table_file_header *tab_header, table_file_header *tab_header_2nd, table_file_header *tab_header_3rd,
	token_list *col_tok_list[MAX_NUM_COL + 1],
	cd_entry *cst_col_1, t_value cst_op_1, token_list *cst_value_1, cd_entry *cst_col_1_2nd, t_value cst_join_cond,
	cd_entry *cst_col_2, t_value cst_op_2, token_list *cst_value_2, cd_entry *cst_col_2_2nd,
	cd_entry *order_col, bool order_asc)
{
	int rc = 0;
	int num_col = 0;
	int num_selected = 0;
	int sum = 0;
	cd_entry *col_entry;
	tpd_entry *tab_entry = tab_header->tpd_ptr;
	tpd_entry *tab_entry_2nd = tab_header_2nd->tpd_ptr;
	tpd_entry *tab_entry_3rd = tab_header_3rd->tpd_ptr;
	rd_entry *record_ptr, *record_ptr_2nd, *record_ptr_3rd;
	int col_len_list[MAX_NUM_COL];
	cd_entry *col_entry_list[MAX_NUM_COL + 1];

	// print file details
	printf("dbfile.bin size = %d\n", g_tpd_list->list_size);
	printf("SELECT statement\n");
	printf("%s.tab size = %d\n", tab_header->tpd_ptr->table_name, tab_header->file_size);

	/////////////////////////////////////////////////////////////////////////////////////
	//////// PRINT HEADING
	/////////////////////////////////////////////////////////////////////////////////////
	// get a col list
	int i = 0;
	for (i = 0; i < MAX_NUM_COL + 1; i++)
	{
		if (col_tok_list[i] != NULL)
		{
			col_entry_list[i] = get_tab_col(tab_entry, tab_entry_2nd, tab_entry_3rd, col_tok_list[i]->tok_string);
			num_col = i;
		}
		else
		{
			col_entry_list[i] = NULL;
		}
	}
	num_col++;
	// get a complete list if not specified
	i = 0;
	if (col_entry_list[0] == NULL)
	{
		cd_entry* col_entry_ptr;
		for (i = 0, col_entry_ptr = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			i < tab_entry->num_columns; i++, col_entry_ptr++)
		{
			col_entry_list[i] = col_entry_ptr;
		}
		int j;
		for (j = 0, col_entry_ptr = (cd_entry*)((char*)tab_entry_2nd + tab_entry_2nd->cd_offset);
			j < tab_entry_2nd->num_columns; j++, col_entry_ptr++)
		{
			col_entry_list[i + j] = col_entry_ptr;
		}
		int k;
		for (k = 0, col_entry_ptr = (cd_entry*)((char*)tab_entry_3rd + tab_entry_3rd->cd_offset);
			k < tab_entry_3rd->num_columns; k++, col_entry_ptr++)
		{
			col_entry_list[i + j + k] = col_entry_ptr;
		}
		num_col = i + j + k;
	}
	// pre-computed the len of each col
	init_col_len_list(col_entry_list, col_len_list);
	printf("\n");
	// print record header
	for (i = 0; i < num_col; i++)
	{
		if (i != 0)
		{
			printf(" ");
		}
		printf("%-*s", col_len_list[i], col_entry_list[i]->col_name);
		printf(" ");
		if (i != num_col - 1)
		{
			printf("|");
		}
	}
	printf("\n");
	// print seperator
	for (i = 0; i < num_col; i++)
	{
		int j;
		for (j = 0; j < col_len_list[i]; j++)
		{
			printf("-");
		}
		// space after a col value
		printf("-");
	}
	// ignore the first space the last seperator
	for (i = 0; i < num_col - 1; i++)
	{
		printf("--");
	}
	printf("\n");

	/////////////////////////////////////////////////////////////////////////////////////
	//////// ORDER the table
	/////////////////////////////////////////////////////////////////////////////////////
	// order the table by col if specified
	if (order_col != NULL)
	{
		table_file_header *order_tab = get_tab_by_col(order_col, tab_header, tab_header_2nd, tab_header_3rd);
		rd_entry *extreme_record = (rd_entry*)calloc(1, order_tab->record_size);
		int order_offset = get_rd_offset_from_record(order_tab->tpd_ptr, order_col);
		int r;
		for (r = 0; r < order_tab->num_records; r++)
		{
			// get the extreme after rth position value and swap to the current r postion
			int extreme_idx = r;
			int i;
			for (i = r + 1, record_ptr = (rd_entry *)((char *)order_tab + order_tab->record_offset + (r + 1) * order_tab->record_size);
				i < order_tab->num_records; i++, record_ptr += order_tab->record_size)
			{
				rd_entry *record_offset = (rd_entry *)((char*)record_ptr + order_offset);
				rd_entry *extreme_offset = (rd_entry *)((char *)order_tab + order_tab->record_offset + extreme_idx * order_tab->record_size + order_offset);

				if (order_col->col_type == T_INT)
				{
					if (order_asc && *((int*)(record_offset + 1)) < *((int*)(extreme_offset + 1)))
					{
						extreme_idx = i;
					}
					else if (!order_asc && *((int*)(record_offset + 1)) > *((int*)(extreme_offset + 1)))
					{
						extreme_idx = i;
					}
				}
				else if (order_col->col_type == T_CHAR)
				{
					char tmp[MAX_CHAR_LEN + 1];
					memset(tmp, '\0', MAX_CHAR_LEN + 1);
					memcpy(tmp, record_offset + 1, (int)*((char *)record_offset));

					char extreme_tmp[MAX_CHAR_LEN + 1];
					memset(extreme_tmp, '\0', MAX_CHAR_LEN + 1);
					memcpy(extreme_tmp, extreme_offset + 1, (int)*((char *)extreme_offset));
					if (order_asc && strcmp(tmp, extreme_tmp) < 0)
					{
						extreme_idx = i;
					}
					else if (!order_asc && strcmp(tmp, extreme_tmp) > 0)
					{
						extreme_idx = i;
					}
				}
			}
			// store the min/max record to tmp memory
			memcpy(extreme_record,
				(rd_entry *)((char *)order_tab + order_tab->record_offset + extreme_idx * order_tab->record_size),
				order_tab->record_size);
			// shift all the records by one position			
			for (i = extreme_idx; i > r; i--)
			{
				rd_entry *cur_record = (rd_entry *)((char *)order_tab + order_tab->record_offset + i * order_tab->record_size);
				rd_entry *prv_record = (rd_entry *)((char *)order_tab + order_tab->record_offset + (i - 1) * order_tab->record_size);
				memcpy(cur_record, prv_record, order_tab->record_size);
			}
			// set the current rth record to the min/max record
			memcpy((rd_entry *)((char *)order_tab + order_tab->record_offset + r * order_tab->record_size),
				extreme_record,
				order_tab->record_size);
		}
		if (extreme_record != NULL)
		{
			free(extreme_record);
		}
		// put order tab in outter loop
		if (order_tab == tab_header_2nd)
		{
			tab_header_2nd = tab_header;
			tab_header = order_tab;
			tab_entry = tab_header->tpd_ptr;
			tab_entry_2nd = tab_header_2nd->tpd_ptr;
		}
		else if (order_tab == tab_header_3rd)
		{
			tab_header_3rd = tab_header;
			tab_header = order_tab;
			tab_entry = tab_header->tpd_ptr;
			tab_entry_3rd = tab_header_3rd->tpd_ptr;
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////
	//////// NESTED LOOP JOIN and print record
	/////////////////////////////////////////////////////////////////////////////////////
	int cst_offset_1, cst_offset_1_2nd, cst_offset_2, cst_offset_2_2nd;
	table_file_header *cst_tab_1, *cst_tab_1_2nd, *cst_tab_2, *cst_tab_2_2nd;
	// init constraint offsets
	if (cst_col_1 != NULL)
	{
		cst_tab_1 = get_tab_by_col(cst_col_1, tab_header, tab_header_2nd, tab_header_3rd);
		// if second arg of the first condition is a col then calculate the offset
		cst_tab_1_2nd = cst_col_1_2nd != NULL ? get_tab_by_col(cst_col_1_2nd, tab_header, tab_header_2nd, tab_header_3rd) : 0;
		cst_offset_1 = get_rd_offset_from_record(cst_tab_1->tpd_ptr, cst_col_1);
		cst_offset_1_2nd = cst_col_1_2nd != NULL ? get_rd_offset_from_record(cst_tab_1_2nd->tpd_ptr, cst_col_1_2nd) : 0;
		if (cst_col_2 != NULL)
		{
			cst_tab_2 = get_tab_by_col(cst_col_2, tab_header, tab_header_2nd, tab_header_3rd);
			// if second arg of the second condition is a col then calculate the offset
			cst_tab_2_2nd = cst_col_2_2nd != NULL ? get_tab_by_col(cst_col_2_2nd, tab_header, tab_header_2nd, tab_header_3rd) : 0;
			cst_offset_2 = get_rd_offset_from_record(cst_tab_2->tpd_ptr, cst_col_2);
			cst_offset_2_2nd = cst_col_2_2nd != NULL ? get_rd_offset_from_record(cst_tab_2_2nd->tpd_ptr, cst_col_2_2nd) : 0;
		}
	}

	int i_2;
	int i_3;
	for (i = 0, record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset);
		i < tab_header->num_records; i++, record_ptr += tab_header->record_size)
	{
		for (i_2 = 0, record_ptr_2nd = (rd_entry *)((char *)tab_header_2nd + tab_header_2nd->record_offset);
			i_2 < tab_header_2nd->num_records; i_2++, record_ptr_2nd += tab_header_2nd->record_size)
		{
			for (i_3 = 0, record_ptr_3rd = (rd_entry *)((char *)tab_header_3rd + tab_header_3rd->record_offset);
				i_3 < tab_header_3rd->num_records; i_3++, record_ptr_3rd += tab_header_3rd->record_size)
			{
				bool valid1, valid2, valid;

				// if there is a constraiant
				if (cst_col_1 != NULL)
				{
					// check join condition
					if (cst_col_1_2nd != NULL)
					{
						valid1 = verify_join_constraiant(
							(rd_entry *)((char*)get_cur_record_by_tab(cst_tab_1, tab_header, tab_header_2nd, tab_header_3rd, record_ptr, record_ptr_2nd, record_ptr_3rd) + 
							cst_offset_1), cst_col_1,
							(rd_entry *)((char*)get_cur_record_by_tab(cst_tab_1_2nd, tab_header, tab_header_2nd, tab_header_3rd, record_ptr, record_ptr_2nd, record_ptr_3rd) + 
							cst_offset_1_2nd), cst_col_1_2nd);
					}
					// check data value condition
					else
					{
						valid1 = verify_constraiant((rd_entry *)((char*)get_cur_record_by_tab(cst_tab_1, tab_header, tab_header_2nd, tab_header_3rd, record_ptr, record_ptr_2nd, record_ptr_3rd) + 
							cst_offset_1), cst_col_1, cst_op_1, cst_value_1);
					}
					if (cst_col_2 != NULL)
					{
						// check join condition
						if (cst_col_2_2nd != NULL)
						{
							valid2 = verify_join_constraiant((rd_entry *)((char*)get_cur_record_by_tab(cst_tab_2, tab_header, tab_header_2nd, tab_header_3rd, record_ptr, record_ptr_2nd, record_ptr_3rd) + 
								cst_offset_2), cst_col_2,
								(rd_entry *)((char*)get_cur_record_by_tab(cst_tab_2_2nd, tab_header, tab_header_2nd, tab_header_3rd, record_ptr, record_ptr_2nd, record_ptr_3rd) + 
								cst_offset_2_2nd), cst_col_2);
						}
						// check data value condition
						else
						{
							valid2 = verify_constraiant((rd_entry *)((char*)get_cur_record_by_tab(cst_tab_2, tab_header, tab_header_2nd, tab_header_3rd, record_ptr, record_ptr_2nd, record_ptr_3rd) +
								cst_offset_2), cst_col_2, cst_op_2, cst_value_2);
						}
					}
				}
				if (cst_join_cond == K_AND)
				{
					// 2 constraint with AND cond
					valid = valid1 && valid2;
				}
				else if (cst_join_cond == K_OR)
				{
					// 2 constraint with OR cond
					valid = valid1 || valid2;
				}
				else if (cst_col_1 != NULL)
				{
					// only 1 constraint, so depends on valid = valid1
					valid = valid1;
				}
				else
				{
					// 0 constraint, valid = true
					valid = true;
				}

				// reset the col value to '\0'
				if (valid)
				{
					num_selected++;
					
					int j;
					for (j = 0; j < num_col; j++)
					{
						table_file_header *target_tab_header = get_tab_by_col(col_entry_list[j], tab_header, tab_header_2nd, tab_header_3rd);
						// if it is the first table
						rd_entry *record_offset = get_cur_record_by_tab(target_tab_header, tab_header, tab_header_2nd, tab_header_3rd, record_ptr, record_ptr_2nd, record_ptr_3rd);
						if (j != 0)
						{
							printf(" ");
						}
						int col_value_offset = get_rd_offset_from_record(target_tab_header->tpd_ptr, col_entry_list[j]);
						int k;
						record_offset = (rd_entry *)((char*)record_offset + col_value_offset);
						if ((int)*((char *)record_offset) == 0)
						{
							if (col_entry_list[j]->col_type == T_INT)
							{
								printf("%*s", col_len_list[j], "-");
							}
							else if (col_entry_list[j]->col_type == T_CHAR)
							{
								printf("%-*s", col_len_list[j], "-");
							}
						}
						else
						{
							if (col_entry_list[j]->col_type == T_INT)
							{
								printf("%*d", col_len_list[j], *((int*)(record_offset + 1)));
							}
							else if (col_entry_list[j]->col_type == T_CHAR)
							{
								char tmp[MAX_COL_DISPLAY_LEN + 1];
								memset(tmp, '\0', MAX_COL_DISPLAY_LEN + 1);
								memcpy(tmp, record_offset + 1, (int)*((char *)record_offset) > col_len_list[j]
									? col_len_list[j] : (int)*((char *)record_offset));
								printf("%-*s", col_len_list[j], tmp);
							}
						}
						printf(" ");
						if (j != num_col - 1)
						{
							printf("|");
						}
					}
					printf("\n");
					
				}
			}
		}
	}
	printf("\n");
	printf("%d rows selected.", num_selected);

	return rc;
}

rd_entry* get_cur_record_by_tab(table_file_header *target, 
	table_file_header *t1, table_file_header *t2, table_file_header *t3,
	rd_entry *r1, rd_entry *r2, rd_entry *r3)
{
	return target == t1 ? r1 : target == t2 ? r2 : r3;
}

int init_col_len_list(cd_entry *col_entry_list[MAX_NUM_COL + 1], int* col_len_list)
{
	int i = 0;
	cd_entry* col_entry;

	while (col_entry_list[i] != NULL)
	{
		col_len_list[i] = get_col_display_len(col_entry_list[i]);
		i++;
	}
	return 0;
}

int get_col_display_len(cd_entry *col_entry)
{
	int col_display_len;
	if (col_entry->col_type == T_INT)
	{
		// max display length of int is 10
		col_display_len = 10 > strlen(col_entry->col_name) ? 10 : strlen(col_entry->col_name);
	}
	else if (col_entry->col_type == T_CHAR)
	{
		col_display_len= col_entry->col_len > strlen(col_entry->col_name) ?
		col_entry->col_len : strlen(col_entry->col_name);
	}
	
	col_display_len = col_display_len > MAX_COL_DISPLAY_LEN ? MAX_COL_DISPLAY_LEN : col_display_len;
	// return a max col length that is less than MAX_COL_DISPLAY_LEN + spacing length
	return col_display_len;
}

int sem_delete(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	table_file_header *tab_header = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword)
		&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		rc = init_tab_header(cur->tok_string, &tab_header);
		if (rc)
		{
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if (cur->tok_value != EOC && cur->tok_value != K_WHERE)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
			}
			else
			{
				cd_entry *cst_col = NULL;
				t_value cst_op = INVALID;
				token_list *cst_value = NULL;

				if (cur->tok_value == K_WHERE)
				{
					cur = cur->next;
					if ((cst_col = get_tab_col(tab_header->tpd_ptr, NULL, NULL, cur->tok_string)) == NULL)
					{
						rc = COLUMN_NOT_EXIST;
						cur->tok_value = INVALID;
					}
					else
					{
						cur = cur->next;
						if ((cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER
							&& cur->tok_value != S_LESS && cur->tok_value != K_IS)
							|| (cur->tok_value == K_IS && cur->next->tok_value != K_NOT && cur->next->tok_value != K_NULL)
							|| (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value != K_NULL))
						{
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
						}
						else
						{
							if (cur->next->tok_value == K_NOT)
							{
								cur = cur->next;
							}
							cst_op = (t_value)cur->tok_value;
							cur = cur->next;
							// check if K_NULL have IS/IS NOT as operator
							if (cur->tok_value == K_NULL && cst_op != K_IS && cst_op != K_NOT)
							{
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							}
							else
							{
								rc = verify_col_value(cst_col, cur, false);
								if (rc)
								{
									cur->tok_value = INVALID;
								}
								else
								{
									if (cur->next->tok_value != EOC)
									{
										rc = INVALID_STATEMENT;
										cur->next->tok_value = INVALID;
									}
									else
									{
										cst_value = cur;
									}
								}
							}
						}
					}
				}
				if (rc)
				{
					cur->tok_value = INVALID;
				}
				else
				{
					rc = delete_table_dat(tab_header, cst_col, cst_op, cst_value);
					if (rc)
					{
						cur->tok_value = INVALID;
					}
				}
			}
		}
	}
	if (tab_header)
	{
		free(tab_header);
		tab_header = NULL;
	}

	return rc;
}

int delete_table_dat(table_file_header *tab_header, cd_entry *cst_col, t_value cst_op, token_list *cst_value)
{
	int rc = 0;
	FILE *fhandle = NULL;
	int old_size = tab_header->file_size;
	char tab_file[MAX_IDENT_LEN + 4 + 1];

	memset(tab_file, '\0', MAX_IDENT_LEN + 4 + 1);
	strcat(tab_file, tab_header->tpd_ptr->table_name);
	strcat(tab_file, ".tab");

	if ((fhandle = fopen(tab_file, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		// if there is no constraiant
		if (cst_col == NULL)
		{
			tab_header->num_records = 0;
		}
		// if there is constraiant
		else
		{
			int cst_offset = get_rd_offset_from_record(tab_header->tpd_ptr, cst_col);
			rd_entry *record_ptr;
			int i;
			for (i = 0, record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset);
				i < tab_header->num_records; i++, record_ptr += tab_header->record_size)
			{
				bool valid = verify_constraiant((rd_entry *)((char*)record_ptr + cst_offset), cst_col, cst_op, cst_value);

				// remove the row
				if (valid)
				{
					// find the last record that is not going to be removed (last invalid record)
					int j = tab_header->num_records;
					rd_entry *last_record_ptr;
					do
					{
						j--;
						last_record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset + tab_header->record_size * j);
					} while (j > i && verify_constraiant((rd_entry *)((char*)last_record_ptr + cst_offset), cst_col, cst_op, cst_value));

					// if current record is not the last valid record, copy it to the current record
					if (i != j)
					{
						memcpy(record_ptr, last_record_ptr, tab_header->record_size);
					}
					tab_header->num_records = j;

				}
			}
		}
		
		// write into the file
		tab_header->file_size = sizeof(table_file_header) + tab_header->num_records * tab_header->record_size;
		tab_header->tpd_ptr = NULL;
		fwrite(tab_header, tab_header->file_size, 1, fhandle);

		fflush(fhandle);
		fclose(fhandle);
		printf("%d record deleted.\n", (old_size - tab_header->file_size) / tab_header->record_size);
		printf("%s size = %d\n", tab_file, old_size);
		printf("%s new size = %d\n", tab_file, tab_header->file_size);
	}

	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	cd_entry *col_entry = NULL;
	token_list *col_value = NULL;
	table_file_header *tab_header = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword)
		&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		rc = init_tab_header(cur->tok_string, &tab_header);
		if (rc)
		{
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if (cur->tok_value != K_SET)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				if ((cur->tok_class != keyword) &&
					(cur->tok_class != identifier) &&
					(cur->tok_class != type_name))
				{
					// Error
					rc = INVALID_COLUMN_NAME;
					cur->tok_value = INVALID;
				}
				else
				{
					if ((col_entry = get_tab_col(tab_header->tpd_ptr, NULL, NULL, cur->tok_string)) == NULL)
					{
						rc = COLUMN_NOT_EXIST;
						cur->tok_value = INVALID;
					}
					else
					{
						cur = cur->next;
						if (cur->tok_value != S_EQUAL)
						{
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
						}
						else
						{
							cur = cur->next;
							rc = verify_col_value(col_entry, cur, true);
							if (rc)
							{
								cur->tok_value = INVALID;
							}
							else
							{
								col_value = cur;
								cur = cur->next;
								if (cur->tok_value != K_WHERE && cur->tok_value != EOC)
								{
									rc = INVALID_STATEMENT;
									cur->tok_value = INVALID;
								}
								else
								{

									cd_entry *cst_col = NULL;
									t_value cst_op = INVALID;
									token_list *cst_value = NULL;
									
									if (cur->tok_value == K_WHERE)
									{
										cur = cur->next;
										if ((cst_col = get_tab_col(tab_header->tpd_ptr, NULL, NULL, cur->tok_string)) == NULL)
										{
											rc = COLUMN_NOT_EXIST;
											cur->tok_value = INVALID;
										}
										else
										{
											cur = cur->next;
											if ((cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER 
												&& cur->tok_value != S_LESS && cur->tok_value != K_IS)
												|| (cur->tok_value == K_IS && cur->next->tok_value != K_NOT && cur->next->tok_value != K_NULL)
												|| (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value != K_NULL))
											{
												rc = INVALID_STATEMENT;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->next->tok_value == K_NOT)
												{
													cur = cur->next;
												}
												cst_op = (t_value)cur->tok_value;
												cur = cur->next;
												// check if K_NULL have IS/IS NOT as operator
												if (cur->tok_value == K_NULL && cst_op != K_IS && cst_op != K_NOT)
												{
													rc = INVALID_STATEMENT;
													cur->tok_value = INVALID;
												}
												else
												{
													rc = verify_col_value(cst_col, cur, false);
													if (rc)
													{
														cur->tok_value = INVALID;
													}
													else
													{
														if (cur->next->tok_value != EOC)
														{
															rc = INVALID_STATEMENT;
															cur->next->tok_value = INVALID;
														}
														else
														{
															cst_value = cur;
														}
													}
												}
											}
										}
									}
									if (!rc)
									{
										rc = update_table_dat(tab_header, col_entry, col_value, cst_col, cst_op, cst_value);
										if (rc)
										{
											cur->tok_value = INVALID;
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	if (tab_header)
	{
		free(tab_header);
		tab_header = NULL;
	}

	return rc;
}

int verify_col_value(cd_entry *col_entry, token_list *t_list, bool set_value)
{
	int rc = 0;
	token_list *cur = t_list;

	if (set_value || cur->tok_value != K_NULL)
	{
		if (col_entry->col_type == T_INT
			&& (cur->tok_value == INT_LITERAL || cur->tok_value == K_NULL))
		{
			if (col_entry->not_null && cur->tok_value == K_NULL)
			{
				rc = COL_NOT_NULL;
			}
			else
			{
				// reject the int if its string length is more than "2147483647" 
				// OR same length but greater than "2147483647" in strcmp
				const char *max = "2147483647";
				int max_len = strlen(max);
				char *str_ptr = cur->tok_string;
				// remove leading zeros
				while (*str_ptr == '0') str_ptr++;

				if (cur->tok_value != K_NULL && 
					((strcmp(str_ptr, max) > 0 && strlen(str_ptr) == max_len) || strlen(str_ptr) > max_len))
				{
					rc = OVERSIZE_DATA;
				}
				else
				{
					// further checking on int
				}
			}
		}
		else if (col_entry->col_type == T_CHAR
			&& (cur->tok_value == STRING_LITERAL || cur->tok_value == K_NULL))
		{
			if (col_entry->not_null && cur->tok_value == K_NULL)
			{
				rc = COL_NOT_NULL;
			}
			else
			{
				if (cur->tok_value != K_NULL && 
					(strlen(cur->tok_string) > col_entry->col_len || strlen(cur->tok_string) > MAX_CHAR_LEN))
				{
					rc = OVERSIZE_DATA;
				}
				else
				{
					// further checking on string
				}
			}
		}
		else
		{
			rc = INVALID_COL_TYPE;
		}
	}
	
	return rc;
}

cd_entry* get_tab_col(tpd_entry *tab_entry, tpd_entry *tab_entry_2nd, tpd_entry *tab_entry_3rd, char* col_name)
{
	int i;
	bool found = false;
	cd_entry *col_entry;

	for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		i < tab_entry->num_columns; i++, col_entry++)
	{
		if (stricmp(col_entry->col_name, col_name) == 0)
		{
			found = true;
			break;
		}
	}

	// if it is not found in the first table, check second table
	if (tab_entry_2nd != NULL && !found)
	{
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry_2nd + tab_entry_2nd->cd_offset);
			i < tab_entry_2nd->num_columns; i++, col_entry++)
		{
			if (stricmp(col_entry->col_name, col_name) == 0)
			{
				found = true;
				break;
			}
		}
	}

	// if it is not found in the second table, check third table
	if (tab_entry_3rd != NULL && !found)
	{
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry_3rd + tab_entry_3rd->cd_offset);
			i < tab_entry_3rd->num_columns; i++, col_entry++)
		{
			if (stricmp(col_entry->col_name, col_name) == 0)
			{
				found = true;
				break;
			}
		}
	}

	if (!found)
	{
		col_entry = NULL;
	}

	return col_entry;
}

table_file_header* get_tab_by_col(cd_entry *col, table_file_header *tab_header, table_file_header *tab_header_2nd, table_file_header *tab_header_3rd)
{
	int i;
	bool found = false;
	table_file_header *target_tab_header = NULL;
	cd_entry *col_entry;

	for (i = 0, col_entry = (cd_entry*)((char*)tab_header->tpd_ptr + tab_header->tpd_ptr->cd_offset);
		i < tab_header->tpd_ptr->num_columns; i++, col_entry++)
	{
		if (stricmp(col_entry->col_name, col->col_name) == 0)
		{
			target_tab_header = tab_header;
			found = true;
			break;
		}
	}

	// if it is not found in the first table, check second table
	if (tab_header_2nd != NULL && !found)
	{
		for (i = 0, col_entry = (cd_entry*)((char*)tab_header_2nd->tpd_ptr + tab_header_2nd->tpd_ptr->cd_offset);
			i < tab_header_2nd->tpd_ptr->num_columns; i++, col_entry++)
		{
			if (stricmp(col_entry->col_name, col->col_name) == 0)
			{
				target_tab_header = tab_header_2nd;
				found = true;
				break;
			}
		}
	}

	// if it is not found in the second table, check third table
	if (tab_header_3rd != NULL && !found)
	{
		for (i = 0, col_entry = (cd_entry*)((char*)tab_header_3rd->tpd_ptr + tab_header_3rd->tpd_ptr->cd_offset);
			i < tab_header_3rd->tpd_ptr->num_columns; i++, col_entry++)
		{
			if (stricmp(col_entry->col_name, col->col_name) == 0)
			{
				target_tab_header = tab_header_3rd;
				found = true;
				break;
			}
		}
	}

	return target_tab_header;
}

int update_table_dat(table_file_header *tab_header, cd_entry *col_entry, token_list* col_value,
					 cd_entry *cst_col, t_value cst_op, token_list *cst_value)
{
	int rc = 0;
	int num_update = 0;
	int col_offset = 0;
	FILE *fhandle = NULL;
	cd_entry *col_entry_ptr;
	rd_entry *record_ptr;
	char tab_file[MAX_IDENT_LEN + 4 + 1];

	memset(tab_file, '\0', MAX_IDENT_LEN + 4 + 1);
	strcat(tab_file, tab_header->tpd_ptr->table_name);
	strcat(tab_file, ".tab");

	if ((fhandle = fopen(tab_file, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		for (col_entry_ptr = (cd_entry*)((char*)tab_header->tpd_ptr + tab_header->tpd_ptr->cd_offset);
			col_entry_ptr < col_entry; col_entry_ptr++)
		{
			col_offset += 1 + col_entry_ptr->col_len;
		}
		int cst_offset;
		if (cst_col != NULL)
		{
			cst_offset = get_rd_offset_from_record(tab_header->tpd_ptr, cst_col);
		}
		int i;
		for (i = 0, record_ptr = (rd_entry *)((char *)tab_header + tab_header->record_offset);
			i < tab_header->num_records; i++, record_ptr += tab_header->record_size)
		{
			bool valid = true;
			// if there is a constraiant
			if (cst_col != NULL)
			{
				valid = verify_constraiant((rd_entry *)((char*)record_ptr + cst_offset), cst_col, cst_op, cst_value);
			}

			// reset the col value to '\0'
			if (valid)
			{
				num_update++;
				rd_entry *offset = (rd_entry *)((char*)record_ptr + col_offset);
				memset((char *)offset, '\0', 1 + col_entry->col_len);
				if (col_value->tok_value != K_NULL)
				{
					if (col_entry->col_type == T_INT)
					{
						int int_tmp = sizeof(int);
						memcpy(offset, &int_tmp, 1);
						int_tmp = atoi(col_value->tok_string);
						memcpy(offset + 1, &int_tmp, sizeof(int));
					}
					else if (col_entry->col_type == T_CHAR)
					{
						int str_len = strlen(col_value->tok_string);
						memcpy(offset, &str_len, 1);
						memcpy(offset + 1, col_value->tok_string, str_len);
					}
				}
			}
		}
		tab_header->tpd_ptr = NULL;
		fwrite(tab_header, tab_header->file_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
		printf("%d record updated.\n", num_update);
		printf("%s size = %d\n", tab_file, tab_header->file_size);
	}

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	/* Open for read */
	if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}
		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}			
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int get_rd_offset_from_record(tpd_entry *tpd, cd_entry *cst_col)
{
	int cst_offset = 0;
	cd_entry *col_entry_ptr;
	for (col_entry_ptr = (cd_entry*)((char*)tpd + tpd->cd_offset);
		col_entry_ptr < cst_col; col_entry_ptr++)
	{
		cst_offset += 1 + col_entry_ptr->col_len;
	}
	return cst_offset;
}

bool verify_constraiant(rd_entry *cst_rd_offset, cd_entry *cst_col, t_value cst_op, token_list *cst_value)
{
	bool valid = false;
	// check constraint
	if (cst_value->tok_value == K_NULL)
	{
		if ((cst_op == K_IS && (int)*((char *)cst_rd_offset) == 0)
			|| (cst_op == K_NOT && (int)*((char *)cst_rd_offset) > 0))
		{
			valid = true;
		}
	}
	else
	{
		if (cst_col->col_type == T_INT)
		{
			if ((int)*((char *)cst_rd_offset) > 0)
			{
				if (cst_op == S_EQUAL && *((int*)(cst_rd_offset + 1)) == atoi(cst_value->tok_string))
				{
					valid = true;
				}
				else if (cst_op == S_GREATER && *((int*)(cst_rd_offset + 1)) > atoi(cst_value->tok_string))
				{
					valid = true;
				}
				else if (cst_op == S_LESS && *((int*)(cst_rd_offset + 1)) < atoi(cst_value->tok_string))
				{
					valid = true;
				}
			}
		}
		else if (cst_col->col_type == T_CHAR)
		{
			if ((int)*((char *)cst_rd_offset) > 0)
			{
				char tmp[MAX_TOK_LEN + 1];
				memset(tmp, '\0', MAX_TOK_LEN + 1);
				memcpy(tmp, cst_rd_offset + 1, (int)*((char *)cst_rd_offset));
				if (cst_op == S_EQUAL && strcmp(tmp, cst_value->tok_string) == 0)
				{
					valid = true;
				}
				else if (cst_op == S_GREATER && strcmp(tmp, cst_value->tok_string) > 0)
				{
					valid = true;
				}
				else if (cst_op == S_LESS && strcmp(tmp, cst_value->tok_string) < 0)
				{
					valid = true;
				}
			}
		}
	}
	return valid;
}

bool verify_join_constraiant(rd_entry *cst_rd_offset, cd_entry *cst_col, rd_entry *cst_rd_offset_2nd, cd_entry *cst_col_2nd)
{
	bool valid = false;
	// for a match, two col must have the same length and the same bits
	if (*((int*)(cst_rd_offset + 1)) == *((int*)(cst_rd_offset_2nd + 1)))
	{
		if (strncmp((char*)cst_rd_offset + 1, (char*)cst_rd_offset_2nd + 1, cst_col->col_len) == 0)
		{
			valid = true;
		}
	}

	return valid;
}

int sem_backup(token_list *t_list)
{
	int rc = 0;
	token_list *cur;

	cur = t_list;

	if ((cur->tok_class != keyword)
		&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			rc = backup_dat(cur->tok_string);
			if (rc)
			{
				cur->tok_value = INVALID;
			}
		}
	}

	return rc;
}

int backup_dat(char *filename)
{
	int rc = 0;
	FILE *fhandle = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int image_size = 0;

	// check if image with the same name exist
	if ((fhandle = fopen(filename,"r")) != NULL)
	{
		rc = IMAGE_ALREADY_EXIST;
		printf("\nError - duplicate backup image name\n");
		fclose(fhandle);
	}
	// check if it is writable
	else if ((fhandle = fopen(filename,"wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		// write tpd_list
		fwrite(g_tpd_list, g_tpd_list->list_size, 1, fhandle);
		image_size += g_tpd_list->list_size;
		int i;
		// append table data
		for (i = 0; i < g_tpd_list->num_tables && !rc; 
			i++, cur = (tpd_entry*)((char*)cur + cur->tpd_size))
		{
			table_file_header *tab_header;
			rc = init_tab_header(cur->table_name, &tab_header);
			
			if (!rc)
			{
				tab_header->tpd_ptr = NULL;
				fwrite(&(tab_header->file_size), 4, 1, fhandle);
				fwrite(tab_header, tab_header->file_size, 1, fhandle);
				image_size += 4 + tab_header->file_size;
			}
			if (tab_header)
			{
				free(tab_header);
				tab_header = NULL;
			}
		}
		fclose(fhandle);
		if (rc)
		{
			// remove the incomplete backup file if necessary
			remove(filename);
		}
		else
		{
			printf("image size = %d\n", image_size);
			rc = add_log(BACKUP, filename);
		}
	}

	return rc;
}

int sem_restore(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	char *filename;
	bool rollforward_start = true;

	cur = t_list;
	if ((cur->tok_class != keyword)
		&& (cur->tok_class != identifier) && (cur->tok_class != type_name))
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		filename = cur->tok_string;
		cur = cur->next;
		// optional without RF clause
		if (cur->tok_value == K_WITHOUT && cur->next->tok_value == K_RF)
		{
			cur = cur->next->next;
			rollforward_start = false;
		}
		if (cur->tok_value == K_WITHOUT && cur->next->tok_value != K_RF)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else if (cur->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		if (!rc)
		{
			rc = restore_dat(filename, rollforward_start);
			if (rc)
			{
				cur->tok_value = INVALID;
			}
		}
	}
	return rc;
}

int restore_dat(char *filename, bool rollforward_start)
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	// check if image with the same name exist
	if ((fhandle = fopen(filename, "rbc")) == NULL)
	{
		rc = IMAGE_NOT_EXIST;
		printf("\nError - transaction log does not contain the backup image name specified.\n");
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("\nimage size = %d\n", file_stat.st_size);
		// copy the backup file to memory
		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
			// write dbfile.bin
			FILE *restore_fp = NULL;
			if ((restore_fp = fopen("dbfile.bin", "wbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				fwrite(g_tpd_list, g_tpd_list->list_size, 1, restore_fp);
				char *tab_ptr = (char *)g_tpd_list + g_tpd_list->list_size;
				fclose(restore_fp);
				tpd_entry *cur_tpd = &(g_tpd_list->tpd_start);
				// write tab data
				while (tab_ptr < (char *)g_tpd_list + file_stat.st_size)
				{
					int tab_size = *((int *)tab_ptr);

					char tab_file[MAX_IDENT_LEN + 4 + 1];
					memset(tab_file, '\0', MAX_IDENT_LEN + 4 + 1);
					strcat(tab_file, cur_tpd->table_name);
					strcat(tab_file, ".tab");

					if ((restore_fp = fopen(tab_file, "wbc")) == NULL)
					{
						rc = FILE_OPEN_ERROR;
						break;
					}
					else
					{
						fwrite(tab_ptr + 4, tab_size, 1, restore_fp);
						fclose(restore_fp);
					}
					
					printf("%s created. size = %d\n", tab_file, tab_size);
					tab_ptr += 4 + tab_size;
					cur_tpd = (tpd_entry*)((char*)cur_tpd + cur_tpd->tpd_size);
				}
				printf("\nrestore completed.\n");
			}
		}

		fclose(fhandle);
	}

	if (!rc)
	{
		if (rollforward_start)
		{
			rc = set_db_flag(ROLLFORWARD_PENDING);
			if (!rc)
			{
				rc = set_rf_start_flag(filename, true);
			}
		}
		else
		{
			rc = set_db_flag(0);
			if (!rc)
			{
				rc = prune_log_after_backup(filename);
			}
		}
	}
	return rc;
}

// turn RF_START flag on/off in the log file
int set_rf_start_flag(char *filename, bool flag)
{
	int rc = 0;
	int log_num_entry = 0;
	int tag_idx = 0;
	char tag[MAX_LOG_ENTRY_LEN + 1];
	char log_entry[MAX_LOG_ENTRY_LEN + 1];
	FILE *src_fp = NULL;
	FILE *dst_fp = NULL;

	// if flag is false : search for RF_START tag
	if (!flag)
	{
		memset(tag, '\0', MAX_LOG_ENTRY_LEN + 1);
		strcat(tag, STRING_RF_START);
	}
	// if flag is true : search for BACKUP tag
	else
	{
		memset(tag, '\0', MAX_LOG_ENTRY_LEN + 1);
		strcat(tag, STRING_BACKUP);
		strcat(tag, " ");
		strcat(tag, filename);
	}

	tag_idx = get_log_offset(tag, &log_num_entry, false);
	if (tag_idx < 0)
	{
		// check if any error on removing RF_START
		if (!flag)
		{
			if (tag_idx == -1)
			{
				rc = RF_START_MISSING;
			}
			else
			{
				rc = tag_idx;
			}
		}
		// check if any error on adding RF_START
		else
		{
			if (tag_idx == -1)
			{
				rc = BACKUP_TAG_MISSING;
			}
			else
			{
				rc = tag_idx;
			}
		}
	}
	else
	{
		// if RF_START flag is on and no transcation after the last backup
		if (flag && tag_idx == log_num_entry - 1)
		{
			// do nothing but set the flag to 0 so that it do not block later querys
			rc = set_db_flag(0);
		}
		else
		{
			// delete the current one and make a new copy
			char log_copy_filename[MAX_IDENT_LEN + 1];
			create_log_copy(log_copy_filename);

			// generate a new db.log
			if ((src_fp = fopen(log_copy_filename, "r")) == NULL || (dst_fp = fopen(LOG_FILENAME, "w")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				int i = 0;
				while (fgets(log_entry, MAX_LOG_ENTRY_LEN, src_fp) && i < log_num_entry)
				{
					// remove the RF_START tag
					if (!flag)
					{
						if (i != tag_idx)
						{
							fputs(log_entry, dst_fp);
						}
					}
					// add RF_START right tag after the backup tag
					else
					{
						fputs(log_entry, dst_fp);
						if (i == tag_idx)
						{
							fputs(STRING_RF_START, dst_fp);
							fputs("\n", dst_fp);
						}
					}
					i++;
				}
			}
			if (src_fp != NULL)
			{
				fclose(src_fp);
			}
			if (dst_fp != NULL)
			{
				fclose(dst_fp);
			}
			// if the program run successfully, delete copy if necessary
			if (!rc)
			{
				remove(log_copy_filename);
			}
		}
	}

	return rc;
}

int prune_log_after_backup(char *filename)
{
	int rc = 0;
	int log_num_entry = 0;
	int log_backup_idx = 0;
	char tag[MAX_LOG_ENTRY_LEN + 1];
	char log_entry[MAX_LOG_ENTRY_LEN + 1];
	FILE *src_fp = NULL;
	FILE *dst_fp = NULL;

	memset(tag, '\0', MAX_LOG_ENTRY_LEN + 1);
	strcat(tag, STRING_BACKUP);
	strcat(tag, " ");
	strcat(tag, filename);

	log_backup_idx = get_log_offset(tag, &log_num_entry, false);
	if (log_backup_idx < 0)
	{
		if (rc == -1)
		{
			rc = BACKUP_TAG_MISSING;
		}
		else
		{
			rc = log_backup_idx;
		}
	}
	else
	{
		// check if new copy of log is required
		if (log_backup_idx < log_num_entry - 1)
		{
			// delete the current one and make a new copy
			char log_copy_filename[MAX_IDENT_LEN + 1];
			create_log_copy(log_copy_filename);
			printf("\n%s has been created.\n", log_copy_filename);

			// generate a new db.log
			if ((src_fp = fopen(log_copy_filename, "r")) == NULL || (dst_fp = fopen(LOG_FILENAME, "w")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				int i = 0;
				while (fgets(log_entry, MAX_LOG_ENTRY_LEN, src_fp) && i <= log_backup_idx)
				{
					fputs(log_entry, dst_fp);
					i++;
				}
				if (src_fp != NULL)
				{
					fclose(src_fp);
				}
				if (dst_fp != NULL)
				{
					fclose(dst_fp);
				}				
			}
		}
	}

	return rc;
}

// validate timestamp before using
int prune_log_after_timestamp(char *timestamp)
{
	int rc = 0;
	int log_num_entry = 0;
	int log_tag_idx = 0;
	char tag[MAX_LOG_ENTRY_LEN + 1];
	char log_entry[MAX_LOG_ENTRY_LEN + 1];
	FILE *src_fp = NULL;
	FILE *dst_fp = NULL;

	memset(tag, '\0', MAX_LOG_ENTRY_LEN + 1);
	strcat(tag, timestamp);

	log_tag_idx = get_log_offset(tag, &log_num_entry, false);
	log_tag_idx = get_log_timestamp_offset(tag);

	if (log_tag_idx > 0)
	{
		// check if new copy of log is required
		if (log_tag_idx < log_num_entry - 1)
		{
			// delete the current one and make a new copy
			char log_copy_filename[MAX_IDENT_LEN + 1];
			create_log_copy(log_copy_filename);
			printf("\n%s has been created.\n", log_copy_filename);

			// generate a new db.log
			if ((src_fp = fopen(log_copy_filename, "r")) == NULL || (dst_fp = fopen(LOG_FILENAME, "w")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				int i = 0;
				while (fgets(log_entry, MAX_LOG_ENTRY_LEN, src_fp) && i <= log_tag_idx)
				{
					fputs(log_entry, dst_fp);
					i++;
				}
				if (src_fp != NULL)
				{
					fclose(src_fp);
				}
				if (dst_fp != NULL)
				{
					fclose(dst_fp);
				}
			}
		}
	}

	return rc;
}

int sem_rollforward(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	char *timestamp = NULL;

	cur = t_list;

	// optional to <timestamp> clause
	if (cur->tok_value == K_TO)
	{
		cur = cur->next;
		if (cur->tok_value != INT_LITERAL || strlen(cur->tok_string) != TIMESTAMP_LEN)
		{
			rc = INVALID_TIMESTAMP;
			cur->tok_value = INVALID;
		}
		else
		{
			timestamp = cur->tok_string;
			cur = cur->next;
		}
	}

	// check EOC
	if (cur->tok_value != EOC)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		rc = rollforward_dat(timestamp);
		if (rc)
		{
			cur->tok_value = INVALID;
		}
	}

	return rc;
}

int rollforward_dat(char *timestamp)
{
	int rc = 0;
	int log_num_entry = 0;
	int log_rf_start_idx = 0;
	int log_timestamp_idx = 0;
	char tag[MAX_LOG_ENTRY_LEN + 1];
	char log_entry[MAX_LOG_ENTRY_LEN + 1];
	FILE *src_fp = NULL;
	FILE *dst_fp = NULL;

	log_rf_start_idx = get_log_offset(STRING_RF_START, &log_num_entry, false);
	if (timestamp != NULL)
	{
		log_timestamp_idx = get_log_timestamp_offset(timestamp);
	}
	else
	{
		log_timestamp_idx = log_num_entry;
	}
	
	if (log_rf_start_idx < 0)
	{
		if (log_rf_start_idx == -1)
		{
			rc = RF_START_MISSING;
		}
		else
		{
			rc = log_rf_start_idx;
		}
	}
	else
	{
		if (log_timestamp_idx < 0)
		{
			if (log_timestamp_idx == -1)
			{
				rc = INVALID_TIMESTAMP;
			}
			else
			{
				rc = log_timestamp_idx;
			}
		}
		// timestamp must be greater than or equal to the last timestamp before the backup tag
		else if (log_timestamp_idx < log_rf_start_idx - 1)
		{
			rc = INVALID_TIMESTAMP;
		}
		else
		{
			// generate a new db.log
			if ((src_fp = fopen(LOG_FILENAME, "r")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				// unlock rollforward pending and set redo_in_process
				// if all statement exec correctly, set the db_flag back to 0
				// if any statement failed, set db_flag back to ROLLFORWARD_PENDING
				rc = set_db_flag(REDO_IN_PROCESS);

				printf("\nstart redo recovery.\n");
				int i = 0;
				while (fgets(log_entry, MAX_LOG_ENTRY_LEN, src_fp) 
					&& i <= log_num_entry && i <= log_timestamp_idx)
				{
					if (i > log_rf_start_idx)
					{
						// only redo TIMESTAMP statement
						if (get_log_entry_type(log_entry) == TIMESTAMP && !rc)
						{
							if (g_tpd_list != NULL)
							{
								free(g_tpd_list);
							}
							rc = initialize_tpd_list();
							if (!rc)
							{
								char *token = strtok(log_entry, "\"");
								token = strtok(NULL, "\"");
								printf("\n\n\nprocessing: %s\n", token);
								rc = execute_statement(token);
							}
						}
					}
					i++;
				}
				if (src_fp != NULL)
				{
					fclose(src_fp);
				}
				if (rc)
				{
					rc = set_db_flag(ROLLFORWARD_PENDING);
				}
				else
				{
					// prune all log after certain timestamp if specified
					if (timestamp != NULL)
					{
						rc = prune_log_after_timestamp(timestamp);
					}
					rc = set_db_flag(0);
					if (!rc)
					{
						// remove RF_START tag in any case even if the redo recovery fail
						rc = set_rf_start_flag(NULL, false);
						// unlock db_flag
						g_tpd_list->db_flags = 0;
					}
				}
			}
		}
	}

	return rc;
}

int verify_db_flags()
{
	int rc = 0;
	int log_num_entry = 0;

	if (g_tpd_list->db_flags == 0)
	{
		int return_code = get_log_offset(STRING_RF_START, &log_num_entry, true);
		if (return_code == -1)
		{
			// no RF_START found
		}
		else if (return_code == FILE_OPEN_ERROR)
		{
			// no log file exist
		}
		else if (return_code == LOGFILE_CORRUPTION)
		{
			rc = LOGFILE_CORRUPTION;
		}
		else if (return_code >= 0)
		{
			g_tpd_list->db_flags = ROLLFORWARD_PENDING;
		}
	}
	
	return rc;
}

int add_log(int cmd, char *content)
{
	int rc = 0;
	FILE *fhandle = NULL;

	if ((fhandle = fopen(LOG_FILENAME, "a")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		// generate timestamp
		time_t clock;
		struct tm *time_format;
		char timestamp[TIMESTAMP_LEN + 1];
		time(&clock);
		time_format = localtime(&clock);
		strftime(timestamp, TIMESTAMP_LEN + 1, "%Y%m%d%H%M%S", time_format);

		// backup logging
		if (cmd == BACKUP)
		{
			fputs(STRING_BACKUP, fhandle);
			fputs(" ", fhandle);
			fputs(content, fhandle);
		}
		// rollforward start logging
		else if (cmd == ROLLFORWARD)
		{
			fputs(STRING_RF_START, fhandle);
		}
		// standard logging
		else
		{
			fputs(timestamp, fhandle);
			fputs(" \"", fhandle);
			fputs(content, fhandle);
			fputs("\"", fhandle);
		}
		fputs("\n", fhandle);
		fclose(fhandle);
		printf("\n%s updated.\n", LOG_FILENAME);
	}

	return rc;
}

int get_log_offset(char *tag, int *log_num_entry, bool integrity_check)
{
	int i = 0;
	int idx = -1;
	FILE *fhandle = NULL;
	char log_entry[MAX_LOG_ENTRY_LEN + 1];

	if ((fhandle = fopen(LOG_FILENAME, "r")) != NULL)
	{
		while (fgets(log_entry, MAX_LOG_ENTRY_LEN, fhandle))
		{
			// remove \n
			int last_char = strlen(log_entry) - 1;
			if ((last_char >= 0)
				&& (log_entry[last_char] == '\n'))
			{
				log_entry[last_char] = '\0';
			}
			// check for integrity
			if (integrity_check)
			{
				int log_type = get_log_entry_type(log_entry);
				if (log_type != BACKUP && log_type != ROLLFORWARD && log_type != TIMESTAMP)
				{
					idx = LOGFILE_CORRUPTION;
					printf("invalid log entry at %s\n", log_entry);
					break;
				}
			}
			// if the length is greater than tag length, remove the tail
			log_entry[strlen(tag)] = '\0';

			if (stricmp(tag, log_entry) == 0)
			{
				idx = i;
			}
			i++;
		}
		*log_num_entry = i;
		fclose(fhandle);
	}
	else
	{
		idx = FILE_OPEN_ERROR;
	}

	return idx;
}

int get_log_timestamp_offset(char *timestamp)
{
	int i = 0;
	int idx = -1;
	FILE *fhandle = NULL;
	char log_entry[MAX_LOG_ENTRY_LEN + 1];

	if ((fhandle = fopen(LOG_FILENAME, "r")) != NULL)
	{
		while (fgets(log_entry, MAX_LOG_ENTRY_LEN, fhandle))
		{
			// remove \n
			int last_char = strlen(log_entry) - 1;
			if ((last_char >= 0)
				&& (log_entry[last_char] == '\n'))
			{
				log_entry[last_char] = '\0';
			}
			// for each pass, set idx to current position
			int log_entry_type = get_log_entry_type(log_entry);
			if (log_entry_type != BACKUP && log_entry_type != ROLLFORWARD && log_entry_type != TIMESTAMP)
			{
				idx = LOGFILE_CORRUPTION;
				break;
			}
			else if (log_entry_type == TIMESTAMP)
			{
				log_entry[strlen(timestamp)] = '\0';
				if (stricmp(timestamp, log_entry) >= 0)
				{
					idx = i;
				}
				else
				{
					break;
				}
			}
			else
			{
				idx = i;
			}
			i++;
		}
		fclose(fhandle);
	}
	else
	{
		idx = FILE_OPEN_ERROR;
	}

	return idx;
}

void create_log_copy(char *log_filename)
{
	FILE *fhandle = NULL;
	char log_seq[MAX_IDENT_LEN + 1];

	int i = 1;
	while (true)
	{
		itoa(i, log_seq, 10);
		memset(log_filename, '\0', MAX_IDENT_LEN + 1);
		strcpy(log_filename, LOG_FILENAME);
		strcat(log_filename, log_seq);

		if ((fhandle = fopen(log_filename, "r")) == NULL)
		{
			rename(LOG_FILENAME, log_filename);
			break;
		}
		else
		{
			fclose(fhandle);
		}
		i++;
	}

}

// return type of the log
int get_log_entry_type(char *log_entry)
{
	int log_entry_type = INVALID;
	char log_entry_tmp[MAX_LOG_ENTRY_LEN + 1];

	if (log_entry != NULL)
	{
		// check for backup tag
		strcpy(log_entry_tmp, log_entry);
		log_entry_tmp[strlen(STRING_BACKUP)] = '\0';
		if (stricmp(STRING_BACKUP, log_entry_tmp) == 0)
		{
			log_entry_type = BACKUP;
		}
		// check for RF_START tag
		strcpy(log_entry_tmp, log_entry);
		log_entry_tmp[strlen(STRING_RF_START)] = '\0';
		if (stricmp(STRING_RF_START, log_entry_tmp) == 0)
		{
			log_entry_type = ROLLFORWARD;
		}
		// check for timestamp tag
		strcpy(log_entry_tmp, log_entry);
		log_entry_tmp[TIMESTAMP_LEN] = '\0';
		if (strlen(log_entry_tmp) == TIMESTAMP_LEN)
		{
			// find valid number
			bool is_int = true;
			int i;
			for (i = 0; i < TIMESTAMP_LEN; i++)
			{
				if (!isdigit(log_entry_tmp[i]))
				{
					is_int = false;
					break;
				}
			}
			if (is_int)
			{
				log_entry_type = TIMESTAMP;
			}
			else
			{
				log_entry_type = INVALID_TIMESTAMP;
			}
		}
	}
	

	return log_entry_type;
}

int execute_statement(char *statement)
{
	int rc = 0;

	token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(statement, &tok_list);

		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class,
				tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc)
		{
			rc = do_semantic(tok_list, statement);
		}
		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr = tmp_tok_ptr;
		}
	}

	return rc;
}

int set_db_flag(int state)
{
	// for consistency, reload the data from the file
	if (g_tpd_list != NULL)
	{
		free(g_tpd_list);
	}
	int rc = initialize_tpd_list();
	FILE *fhandle = NULL;
	g_tpd_list->db_flags = state;

	if (!rc){
		if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			// write the current g_tpd_list to file
			fwrite(g_tpd_list, g_tpd_list->list_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
		}
	}
	
	return rc;

}