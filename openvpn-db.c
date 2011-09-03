#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>

#include <sqlite3.h>

typedef enum {
	init, show, read_, get, list,
	put_file, get_file, delete_file, list_files,
	attach_file, detach_file, list_attached,
	extract
} verb_t;

typedef struct named_verb {
	const char   *const name;
	const verb_t        verb;
} named_verb_t;

verb_t verb = 0;
const char *db_path = NULL;
sqlite3 *db = NULL;


void usage(const char *name) {
	fprintf(stderr, "usage: %s init          <DB>\n", name);
	fprintf(stderr, "       %s show          <DB> <NAME>\n", name);
	fprintf(stderr, "       %s read          <DB> <NAME>\n", name);
	fprintf(stderr, "       %s get           <DB> <NAME> <PARAM>\n", name);
	fprintf(stderr, "       %s list          <DB>\n", name);
	
	fprintf(stderr, "       %s put-file      <DB> <FILE>\n", name);
	fprintf(stderr, "       %s get-file      <DB> <FILE>\n", name);
	fprintf(stderr, "       %s delete-file   <DB> <FILE>\n", name);
	fprintf(stderr, "       %s list-files    <DB>\n", name);

	fprintf(stderr, "       %s attach-file   <DB> <NAME> <FILE>\n", name);
	fprintf(stderr, "       %s detach-file   <DB> <NAME> <FILE>\n", name);
	fprintf(stderr, "       %s list-attached <DB> <NAME>\n", name);

	fprintf(stderr, "       %s extract       <DB> <NAME>\n", name);
	exit(EX_USAGE);
}

int cmp_verb(const void *a, const void *b) {
	const named_verb_t *const restrict v1 = (const named_verb_t*) a;
	const named_verb_t *const restrict v2 = (const named_verb_t*) b;
	return strcmp(v1->name, v2->name);
}

int get_verb(const char *name) {
	const named_verb_t verbs[] = { // keep sorted
		{ .name = "attach-file",
		  .verb = attach_file },
		{ .name = "delete-file",
		  .verb = delete_file },
		{ .name = "detach-file",
		  .verb = detach_file },
		{ .name = "extract",
		  .verb = extract },
		{ .name = "get",
		  .verb = get },
		{ .name = "get-file",
		  .verb = get_file },
		{ .name = "init",
		  .verb = init },
		{ .name = "list",
		  .verb = list },
		{ .name = "list-attached",
		  .verb = list_attached },
		{ .name = "list-files",
		  .verb = list_files },
		{ .name = "put-file",
		  .verb = put_file },
		{ .name = "read",
		  .verb = read_ },
		{ .name = "show",
		  .verb = show }
	};
	const named_verb_t key = { .name = name, .verb = 0 };
	const named_verb_t *const found = (const named_verb_t*) bsearch(&key, &verbs, sizeof(verbs) / sizeof(named_verb_t), sizeof(named_verb_t), cmp_verb);
	verb = found ? found->verb : -1;
	return !found;
}

void close_db(void) {
	if ( db == NULL )
		return;

	switch ( sqlite3_close(db) ) {
		case SQLITE_OK: break;
		case SQLITE_BUSY:
			fputs("Failed to close database. The database is busy.\n", stderr);
			break;
		default:
			fputs("Failed to close database.\n", stderr);
			break;
	}
}

void get_db(int argc, const char *argv[]) {
	if ( argc < 3 )
		usage(argv[0]);
	
	atexit(close_db);
	db_path = argv[2];
	if ( sqlite3_open(db_path, &db) != SQLITE_OK ) {
		fprintf(stderr, "failed to open database : %s\n", sqlite3_errmsg(db));
		exit(EX_IOERR);
	}
}

const char *init_sql =
	"CREATE TABLE IF NOT EXISTS Params (\n"
	"    Name  STRING NOT NULL,\n"
	"    Param STRING NOT NULL,\n"
	"    Value STRING,\n"
	"    PRIMARY KEY ( Name, Param )\n"
	");\n"
	"CREATE TABLE IF NOT EXISTS Files (\n"
	"    Name    STRING NOT NULL,\n"
	"    Content BLOB NOT NULL,\n"
	"    PRIMARY KEY ( Name )\n"
	");\n"
	"CREATE TABLE IF NOT EXISTS Edges (\n"
	"    Name STRING NOT NULL,\n"
	"    File STRING NOT NULL,\n"
	"    PRIMARY KEY ( Name, File )\n"
	");\n"
	"CREATE INDEX IF NOT EXISTS ParamByName    ON Params ( Name  );\n"
	"CREATE INDEX IF NOT EXISTS ParamByParam   ON Params ( Param );\n"
	"CREATE INDEX IF NOT EXISTS ParamByPrimary ON Params ( Name, Param );\n"
	"CREATE INDEX IF NOT EXISTS FileByName     ON Files  ( Name );\n"
	"CREATE INDEX IF NOT EXISTS EdgeByName     ON Edges  ( Name );\n"
	"CREATE INDEX IF NOT EXISTS EdgeByFile     ON Edges  ( File );\n"
	"CREATE INDEX IF NOT EXISTS EdgeByPrimary  ON Edges  ( Name, File );\n";

void init_db() {
	char *err = NULL;
	if ( sqlite3_exec(db, init_sql, NULL, NULL, &err) != SQLITE_OK ) {
		fprintf(stderr, "failed to create schema : %s\n", sqlite3_errmsg(db));
		exit(EX_SOFTWARE);
	}
}

void show_conf(int argc, const char *argv[]) {
	sqlite3_stmt *select_name;
	int is_empty = 1;

	if ( argc != 4 )
		usage(argv[0]);

	if ( sqlite3_prepare_v2(db, "SELECT Param, Value FROM Params WHERE Name = ?;", -1, &select_name, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_name);
		exit(EX_SOFTWARE);
	}
		
	if ( sqlite3_bind_text(select_name, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_name);
		exit(EX_SOFTWARE);
	}

	while ( 1 ) {
        	switch ( sqlite3_step(select_name) ) {
			case SQLITE_DONE:
				sqlite3_finalize(select_name);
				if ( is_empty ) {
                                	fprintf(stderr, "Their is no config named \"%s\".\n", argv[3]);
					exit(1);
				}
				return;

			case SQLITE_ROW: {
				const unsigned char *param = sqlite3_column_text(select_name, 0);
				const unsigned char *value = sqlite3_column_text(select_name, 1);
				is_empty = 0;
				
				if ( value != NULL && printf("%s %s\n", param, value) < 0 ) {
					sqlite3_finalize(select_name);
					fputs("failed to write to standard output.\n", stderr);
					exit(EX_IOERR);
				}
				if ( value == NULL && printf("%s\n", param) < 0 ) {
					sqlite3_finalize(select_name);
					fputs("failed to write to standard output.\n", stderr);
					exit(EX_IOERR);
				}
				break;
			}

			default:
				fprintf(stderr, "failed to step trough result set : %s\n", sqlite3_errmsg(db));
				sqlite3_finalize(select_name);
				exit(EX_SOFTWARE);
				break;
		}
	}
}

void read_conf(int argc, const char *argv[]) {
	char *line = NULL;
	size_t linecap = 0;
	ssize_t linelen;
	sqlite3_stmt *insert_param = NULL;
	char *err = NULL;

	if ( argc != 4 ) {
		usage(argv[0]);
	}

	if ( sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO Params ( Name, Param, Value ) VALUES ( ?, ?, ? );", -1, &insert_param, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_param);
		exit(EX_SOFTWARE);
	}

	if ( sqlite3_bind_text(insert_param, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
        	fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_param);
		exit(EX_SOFTWARE);
	}
	
	if ( sqlite3_exec(db, "BEGIN;", NULL, NULL, &err) != SQLITE_OK ) {
		fprintf(stderr, "failed to begin commit : %s\n", sqlite3_errmsg(db));
		exit(EX_SOFTWARE);
	}
	
	while ( (linelen = getline(&line, &linecap, stdin)) > 0 ) {
		char *value = line, c;
		if ( !strsep(&value, "#;\n") ) continue; // ignore comments and newlines
		value = line;
		strsep(&value, " \t");
		while (value && (c = *value, c == ' ' || c == '\t')) value++;
		if ( strlen(line) == 0 ) continue;

		if ( sqlite3_bind_text(insert_param, 2, line, -1, SQLITE_TRANSIENT) != SQLITE_OK ) {
			fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
			exit(EX_SOFTWARE);
		}
		if ( value && sqlite3_bind_text(insert_param, 3, value, -1, SQLITE_TRANSIENT) != SQLITE_OK ) {
                	fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
			exit(EX_SOFTWARE);
		}
		if ( !value && sqlite3_bind_null(insert_param, 3) != SQLITE_OK ) {
			fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
			exit(EX_SOFTWARE);
		}

		if ( sqlite3_step(insert_param) != SQLITE_DONE ) {
			fprintf(stderr, "failed to insert into table : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
			exit(EX_SOFTWARE);
		}

		if ( sqlite3_reset(insert_param) != SQLITE_OK ) {
			fprintf(stderr, "failed to reset insert statement : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
			exit(EX_SOFTWARE);
		}
	}
	sqlite3_finalize(insert_param);
	if ( line != NULL )
		free(line);

	if ( ferror(stdin) ) {
        	fprintf(stderr, "failed to read from stdin.\n");
		sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
		exit(EX_IOERR);
	}

	if ( sqlite3_exec(db, "COMMIT;", NULL, NULL, &err) != SQLITE_OK ) {
		fprintf(stderr, "failed to commit transaction : %s\n", err);
		sqlite3_finalize(insert_param);
		sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
		exit(EX_SOFTWARE);
	}
}

const char *get_sql =
	"SELECT ( SELECT COUNT(*) FROM Params WHERE Name = ?1 ),\n"
        "       ( SELECT Value FROM Params WHERE Name = ?1 AND Param = ?2 );";

void get_conf(int argc, const char *argv[]) {
	sqlite3_stmt *select_param;

	if ( argc != 5 )
		usage(argv[0]);
	
	if ( sqlite3_prepare_v2(db, get_sql, -1, &select_param, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_param);
		exit(EX_SOFTWARE);
	}
        
	if ( sqlite3_bind_text(select_param, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_param);
		exit(EX_SOFTWARE);
	}

	if ( sqlite3_bind_text(select_param, 2, argv[4], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_param);
		exit(EX_SOFTWARE);
	}

	if ( sqlite3_step(select_param) != SQLITE_ROW ) {
		fprintf(stderr, "logic error in get_conf().\n");
		sqlite3_finalize(select_param);
		exit(EX_SOFTWARE);
	}

	sqlite3_int64 count = sqlite3_column_int64(select_param, 0);
	const unsigned char *value = sqlite3_column_text(select_param, 1);

	if ( !count ) {
		fprintf(stderr, "their is no config named \"%s\".\n", argv[3]);
		sqlite3_finalize(select_param);
		exit(1);
	}

	if ( !value ) {
		fprintf(stderr, "their is parameter named \"%s\" in the config named \"%s\".\n", argv[4], argv[3]);
		sqlite3_finalize(select_param);
		exit(2);
	}

	if ( puts((const char*)value) == EOF ) {
		fprintf(stderr, "failed to write to stdout");
		sqlite3_finalize(select_param);
		exit(EX_IOERR);
	}

	sqlite3_finalize(select_param);
}

void list_conf(int argc, const char *argv[]) {
	sqlite3_stmt *select_conf = NULL;

	if ( argc != 3 )
		usage(argv[0]);
	
	if ( sqlite3_prepare_v2(db, "SELECT DISTINCT Name FROM Params ORDER BY Name ASC;", -1, &select_conf, NULL ) != SQLITE_OK ) {
		fprintf(stderr, "failed to pepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_conf);
		exit(EX_SOFTWARE);
	}

        while ( 1 ) {
		switch ( sqlite3_step(select_conf) ) {
                        case SQLITE_DONE:
				sqlite3_finalize(select_conf);
				return;
			
			case SQLITE_ROW: {
				const unsigned char *name = sqlite3_column_text(select_conf, 0);
				if ( puts((const char*)name) == EOF ) {
					sqlite3_finalize(select_conf);
					fputs("failed to write to standard output.\n", stderr);
				}
				break;
			}

			default:
				fprintf(stderr, "failed to step trough result set : %s\n", sqlite3_errmsg(db));
				sqlite3_finalize(select_conf);
				exit(EX_SOFTWARE);
				break;
		}
	}
}

int copy_file(int src_fd, int dst_fd, uint64_t *len) {
	int eof = 0;
	uint64_t len_ = 0;
	do {            
		uint8_t buf[128*1024];
		const struct timespec _10ms = { .tv_sec = 0, .tv_nsec = 10000000 };
		size_t i = 0;
		ssize_t delta;
		do {
			delta = read(src_fd, buf + i, sizeof(buf) - i);
			if ( delta > 0 ) {
                        	i += delta;
			} else if ( delta == 0 ) {
				eof = 1;
			} else { 
				switch ( errno ) {
					case EAGAIN:
						nanosleep(&_10ms, NULL);

					case EINTR:
						break;

					default:
						return 1;
				}
			}
		} while ( !eof && i != sizeof(buf) );

		size_t j = 0;
		while ( j < i ) {
			delta = write(dst_fd, buf + j, i - j);
			if ( delta >= 0 ) {
				j += delta;
				len_ += delta;
			} else {
                                switch ( errno ) {
					case EAGAIN:
						nanosleep(&_10ms, NULL);
					
					case EINTR:
						break;
					
					default:
						return 2;
				}
			}
		}
	} while ( !eof );
	if ( len ) *len = len_;

	return 0;
}

void write_blob(sqlite3_blob *blob, int src_fd) {
	int eof = 0;
	int off = 0;
	do {
		uint8_t buf[128*1024];
		const struct timespec _10ms = { .tv_sec = 0, .tv_nsec = 10000000 };
		size_t i = 0;
		ssize_t delta;

		do {
			delta = read(src_fd, buf + i, sizeof(buf) - i );
			if ( delta > 0 ) {
				i += delta;
			} else if ( delta == 0 ) {
				eof = 1;
			} else {
				switch ( errno ) {
					case EAGAIN:
						nanosleep(&_10ms, NULL);

					case EINTR:
						break;

					default:
                                                perror("failed to read from fd");
						sqlite3_blob_close(blob);
						exit(EX_IOERR);
						break;
				}
			}
		} while ( !eof && i != sizeof(buf) );

		if ( sqlite3_blob_write(blob, buf, i, off) != SQLITE_OK ) {
			fprintf(stderr, "failed to write to blob : %s\n", sqlite3_errmsg(db));
			sqlite3_blob_close(blob);
			exit(EX_IOERR);
		}

		off += delta;
	} while ( !eof );
}

void store_file(int argc, const char *argv[]) {
	if ( argc != 4 ) 
		usage(argv[0]);
	
	const char *tmp_template = "/tmp/openvpn-db.XXXXXXXX";
	char tmp_name[PATH_MAX];
	strncpy(tmp_name, tmp_template, PATH_MAX);
	int tmp_fd = mkstemp(tmp_name);
	if ( tmp_fd == -1 ) {
		perror("failed to create temporary file");
		exit(EX_OSERR);
	}
	if ( unlink(tmp_name) ) {
		perror("failed to unlink emporary file");
		exit(EX_OSERR);
	}
	
	uint64_t len;
	if ( copy_file(STDIN_FILENO, tmp_fd, &len) ) {
		perror("failed to copy stdin to temporary file");
		exit(EX_IOERR);
	}

	if ( lseek(tmp_fd, 0, SEEK_SET) == -1 ) {
        	perror("failed to rewind temporaray file");
		exit(EX_IOERR);
	}
	
	sqlite3_stmt *insert_file = NULL;
	if ( sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO Files ( Name, Content ) VALUES ( ?, ? );", -1, &insert_file, NULL) != SQLITE_OK ) {
        	fprintf(stderr, "failed to prepare statment : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_file);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_text(insert_file, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_file);
		exit(EX_SOFTWARE);
	}
	if ( !(len <= (uint64_t)INT_MAX) ) {
		fprintf(stderr, "the SQLite 3 API doesn't support blobs larger than INT_MAX. Length of %" PRIu64 " is larger than %i.\n", len, INT_MAX);
		sqlite3_finalize(insert_file);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_zeroblob(insert_file, 2, (int) len) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_file);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_step(insert_file) != SQLITE_DONE ) {
		fprintf(stderr, "failed to insert into table : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_file);
		exit(EX_SOFTWARE);
	}
	sqlite3_finalize(insert_file);
	
	sqlite3_blob *blob = NULL;
	sqlite3_int64 row_id = sqlite3_last_insert_rowid(db);
	if ( sqlite3_blob_open(db, "main", "Files", "Content", row_id, 1, &blob) != SQLITE_OK ) {
		fprintf(stderr, "failed to open blob for writing : %s\n", sqlite3_errmsg(db));
		sqlite3_blob_close(blob);
		exit(EX_SOFTWARE);
	}
	
	write_blob(blob, tmp_fd);
	sqlite3_blob_close(blob);
}

void read_blob(sqlite3_blob *blob, int dst_fd) {
	int len = sqlite3_blob_bytes(blob);
	int off = 0;
	uint8_t buf[128*1024];
	const struct timespec _10ms = { .tv_sec = 0, .tv_nsec = 10000000 };

	while ( off < len ) {
		int n = sizeof(buf) > len - off ? len - off : sizeof(buf);
		if ( sqlite3_blob_read(blob, buf, n, off) != SQLITE_OK ) {
			fprintf(stderr, "failed to read from blob : %s\n", sqlite3_errmsg(db));
			sqlite3_blob_close(blob);
			exit(EX_IOERR);
		}

		size_t j = 0;
		while ( j < n ) {
                	ssize_t delta = write(dst_fd, buf + j, n - j);
			if ( delta >= 0 ) {
				j += delta;
			} else {
				switch ( errno ) {
					case EAGAIN:
						nanosleep(&_10ms, NULL);

					case EINTR:
						break;
					
					default:
						perror("failed to write to fd");
						sqlite3_blob_close(blob);
						exit(EX_IOERR);
						break;
				}
			}
		}
		off += n;
	}
}

void retrieve_file(int argc, const char *argv[]) {
	if ( argc != 4 )
		usage(argv[0]);
	
	sqlite3_stmt *select_file = NULL;

	if ( sqlite3_prepare_v2(db, "SELECT _rowid_ FROM Files WHERE Name = ?;", -1, &select_file, NULL) != SQLITE_OK ) {
        	fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_file);
		exit(EX_SOFTWARE);
	}

	if ( sqlite3_bind_text(select_file, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
        	fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_file);
		exit(EX_SOFTWARE);
	}

	sqlite3_int64 row_id = -1;
	switch ( sqlite3_step(select_file) ) {
		case SQLITE_ROW:
			row_id = sqlite3_column_int64(select_file, 0);
			break;
		
		case SQLITE_DONE:
			fprintf(stderr, "Their is no file named \"%s\n store in the database.\n", argv[3]);
			sqlite3_finalize(select_file);
			exit(1);
			break;

		default:
			fprintf(stderr, "failed to select file : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(select_file);
			exit(EX_SOFTWARE);
			break;
	}
	sqlite3_finalize(select_file);

	sqlite3_blob *blob = NULL;
	if ( sqlite3_blob_open(db, "main", "Files", "Content", row_id, 0, &blob) != SQLITE_OK ) {
        	fprintf(stderr, "failed to open blob for reading : %s\n", sqlite3_errmsg(db));
		sqlite3_blob_close(blob);
		exit(EX_SOFTWARE);
	}
	
	read_blob(blob, STDOUT_FILENO);
	sqlite3_blob_close(blob);
}

void ls(int argc, const char *argv[]) {
	if ( argc != 3 )
		usage(argv[0]);
	
	sqlite3_stmt *select_files = NULL;
	if ( sqlite3_prepare_v2(db, "SELECT LENGTH(Content), Name FROM Files;", -1, &select_files, NULL) != SQLITE_OK ) {
        	fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_files);
		exit(EX_SOFTWARE);
	}

	int is_empty = 1;
	while ( 1 ) {
		switch ( sqlite3_step(select_files) ) {
        		case SQLITE_DONE:
				sqlite3_finalize(select_files);
				if ( is_empty ) {
					fprintf(stderr, "Their are no files stored in the database.\n");
					exit(1);
				}
				return;
			
			case SQLITE_ROW: {
				const int            len  = sqlite3_column_int(select_files, 0);
				const unsigned char *name = sqlite3_column_text(select_files, 1);
				is_empty = 0;

                                if ( printf("%11i\t%s\n", len, name) < 0 ) {
					sqlite3_finalize(select_files);
					fputs("failed to write to standard output.\n", stderr);
					exit(EX_IOERR);
				}
				break;
			}

			default:
				fprintf(stderr, "failed to step trough result set : %s\n", sqlite3_errmsg(db));
				sqlite3_finalize(select_files);
				exit(EX_SOFTWARE);
				break;
		}

	}
}

void del(int argc, const char *argv[]) {
	if ( argc != 4 )
		usage(argv[0]);

	if ( sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed to begin commit : %s\n", sqlite3_errmsg(db));
		exit(EX_SOFTWARE);
	}
	
	sqlite3_stmt *select_edge = NULL;
	if ( sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM Edges WHERE File = ?;", -1, &select_edge, NULL) != SQLITE_OK ) {
        	fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_text(select_edge, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		exit(EX_SOFTWARE);
	}

	sqlite3_stmt *delete_file = NULL;
	if ( sqlite3_prepare_v2(db, "DELETE FROM Files WHERE Name = ?;", -1, &delete_file, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed to perpare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		sqlite3_finalize(delete_file);
		exit(EX_SOFTWARE);
	}

	if ( sqlite3_bind_text(delete_file, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		sqlite3_finalize(delete_file);
		exit(EX_SOFTWARE);
	}

	sqlite3_stmt *count_file = NULL;
	if ( sqlite3_prepare_v2(db, "SELECT COUNT (*) FROM Files WHERE Name = ?;", -1, &count_file, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		sqlite3_finalize(delete_file);
		sqlite3_finalize(count_file);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_text(count_file, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		sqlite3_finalize(delete_file);
		sqlite3_finalize(count_file);
		exit(EX_SOFTWARE);
	}
	
	int count = 0;
	if ( sqlite3_step(select_edge) == SQLITE_ROW ) {
		count = sqlite3_column_int(select_edge, 0);
	} else {
                fprintf(stderr, "failed to count the edges attached to this file : %s\n", sqlite3_errmsg(db));
                sqlite3_finalize(select_edge);
		sqlite3_finalize(delete_file);
		sqlite3_finalize(count_file);
		exit(EX_SOFTWARE);
	}
        sqlite3_finalize(select_edge);
	if ( count != 0 ) {
		fprintf(stderr, "The file named \"%s\" is attached to configs.\n", argv[3]);
		sqlite3_finalize(delete_file);
		sqlite3_finalize(count_file);
		exit(1);
	}

	int present = 0;
	if ( sqlite3_step(count_file) == SQLITE_ROW ) {
		present = sqlite3_column_int(count_file, 0);
	} else {
		fprintf(stderr, "failed to count file : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(delete_file);
		sqlite3_finalize(count_file);
		exit(EX_SOFTWARE);
	}
	sqlite3_finalize(count_file);
	if ( !present ) {
        	fprintf(stderr, "Their is no file named \"%s\" to delete.\n", argv[3]);
		sqlite3_finalize(delete_file);
		exit(2);
	}

	if ( sqlite3_step(delete_file) != SQLITE_DONE ) {
        	fprintf(stderr, "failed to delete the file named \"%s\" : %s\n", argv[3], sqlite3_errmsg(db));
		sqlite3_finalize(delete_file);
		exit(EX_SOFTWARE);
	}
	sqlite3_finalize(delete_file);

	if ( sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed to commit transaction %s\n", sqlite3_errmsg(db));
		exit(EX_SOFTWARE);
	}
}

void add_edge(int argc, const char *argv[]) {
	if ( argc != 5 )
		usage(argv[0]);

	sqlite3_stmt *insert_edge = NULL;
	if ( sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO Edges ( Name, File ) VALUES ( ?, ? );", -1, &insert_edge, NULL) != SQLITE_OK ) {
        	fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_edge);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_text(insert_edge, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_edge);
		exit(EX_SOFTWARE);
	}
        if ( sqlite3_bind_text(insert_edge, 2, argv[4], -1, SQLITE_STATIC) != SQLITE_OK ) {
        	fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_edge);
		exit(EX_SOFTWARE);
	}

	if ( sqlite3_step(insert_edge) != SQLITE_DONE ) {
		fprintf(stderr, "failed to insert edge : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(insert_edge);
		exit(EX_SOFTWARE);
	}
	sqlite3_finalize(insert_edge);
}

void del_edge(int argc, const char *argv[]) {
	if ( argc != 5 )
		usage(argv[0]);
	
	sqlite3_stmt *delete_edge = NULL;
	if ( sqlite3_prepare_v2(db, "DELETE FROM Edges WHERE Name = ? AND File = ?;", -1, &delete_edge, NULL) != SQLITE_OK ) {
        	fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(delete_edge);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_text(delete_edge, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(delete_edge);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_text(delete_edge, 2, argv[4], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(delete_edge);
		exit(EX_SOFTWARE);
	}

	if ( sqlite3_step(delete_edge) != SQLITE_DONE ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(delete_edge);
		exit(EX_SOFTWARE);
	}
	sqlite3_finalize(delete_edge);
}

void list_edges(int argc, const char *argv[]) {
	if ( argc != 4 )
		usage(argv[0]);

	if ( sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL) != SQLITE_OK ) {
		fprintf(stderr, "failed begin transaction : %s\n", sqlite3_errmsg(db));
		exit(EX_SOFTWARE);
	}
	
	sqlite3_stmt *select_edge = NULL;
	if ( sqlite3_prepare_v2(db, "SELECT File FROM Edges WHERE Name = ?;", -1, &select_edge, NULL) != SQLITE_OK ) {
        	fprintf(stderr, "failed to prepare statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
		exit(EX_SOFTWARE);
	}
	if ( sqlite3_bind_text(select_edge, 1, argv[3], -1, SQLITE_STATIC) != SQLITE_OK ) {
		fprintf(stderr, "failed to bind parameter to statement : %s\n", sqlite3_errmsg(db));
		sqlite3_finalize(select_edge);
		sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
		exit(EX_SOFTWARE);
	}
	
	int is_empty = 1;
	while ( 1 ) {
		switch ( sqlite3_step(select_edge) ) {
			case SQLITE_DONE:
				sqlite3_finalize(select_edge);
				sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
				if ( is_empty ) {
					fprintf(stderr, "Their is no file attached to the config named \"%s\".\n", argv[3]);
					exit(1);
				}
				return;
			
			case SQLITE_ROW: {
				const unsigned char *file = sqlite3_column_text(select_edge, 0);
				is_empty = 0;

                                if ( printf("%s\n", file) < 0 ) {
					sqlite3_finalize(select_edge);
					fputs("failed to write to standard output.\n", stderr);
					exit(EX_IOERR);
				}
				break;
			}

			default:
				fprintf(stderr, "failed to step trough result set : %s\n", sqlite3_errmsg(db));
				sqlite3_finalize(select_edge);
				sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
				exit(EX_SOFTWARE);
				break;
		}
	}
}                                            

int main(int argc, const char *argv[]) {
	if ( argc < 2 || get_verb(argv[1]) )
		usage(argv[0]);

	get_db(argc, argv);
	init_db();
	switch ( verb ) {
        	case init:
			break;
		
		case show:
			show_conf(argc, argv);
			break;

		case read_:
			read_conf(argc, argv);
			break;

		case get:
			get_conf(argc, argv);
			break;

		case list:
			list_conf(argc, argv);
			break;

		case put_file:
			store_file(argc, argv);
			break;

		case get_file:
			retrieve_file(argc, argv);
			break;
		
		case list_files:
			ls(argc, argv);
			break;

		case delete_file:
			del(argc, argv);
			break;

		case attach_file:
			add_edge(argc, argv);
			break;
		
		case detach_file:
			del_edge(argc, argv);
			break;
		
		case list_attached:
			list_edges(argc, argv);
			break;
		
		case extract:
			break;

		default:
			usage(argv[0]);
			break;
	}
	return 0;
}
