#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>

#include <sqlite3.h>

typedef enum {
	init, show, read
} verb_t;

verb_t verb = 0;
const char *db_path = NULL;
sqlite3 *db = NULL;


void usage(const char *name) {
	fprintf(stderr, "usage: %s init <DB>\n", name);
	fprintf(stderr, "       %s show <DB> <NAME>\n", name);
	fprintf(stderr, "       %s read <DB> <NAME>\n", name);
	exit(EX_USAGE);
}

int get_verb(const char *name) {
	if (!strcmp(name, "init")) {
		verb = init;
		return 0;
	}
	if (!strcmp(name, "show")) {
		verb = show;
		return 0;
	}
	if (!strcmp(name, "read")) {
		verb = read;
		return 0;
	}
	return -1;
}

void close_db(void) {
	if ( db == NULL )
		return;

	switch ( sqlite3_close(db) != SQLITE_OK ) {
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
	"CREATE INDEX IF NOT EXISTS ParamByName  ON Params ( Name  );\n"
	"CREATE INDEX IF NOT EXISTS ParamByParam ON Params ( Param );\n";

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
					exit(EX_IOERR);
				}
				if ( value == NULL && printf("%s\n", param) < 0 ) {
					sqlite3_finalize(select_name);
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
			exit(EX_SOFTWARE);
		}
		if ( value && sqlite3_bind_text(insert_param, 3, value, -1, SQLITE_TRANSIENT) != SQLITE_OK ) {
                	fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			exit(EX_SOFTWARE);
		}
		if ( !value && sqlite3_bind_null(insert_param, 3) != SQLITE_OK ) {
			fprintf(stderr, "failed to bind parameter : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			exit(EX_SOFTWARE);
		}

		if ( sqlite3_step(insert_param) != SQLITE_DONE ) {
			fprintf(stderr, "failed to insert into tabel : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			exit(EX_SOFTWARE);
		}

		if ( sqlite3_reset(insert_param) != SQLITE_OK ) {
			fprintf(stderr, "failed to reset insert statement : %s\n", sqlite3_errmsg(db));
			sqlite3_finalize(insert_param);
			exit(EX_SOFTWARE);
		}
	}
	sqlite3_finalize(insert_param);
	if ( line != NULL )
		free(line);

	if ( ferror(stdin) ) {
        	fprintf(stderr, "failed to read from stdin.\n");
		exit(EX_IOERR);
	}

	if ( sqlite3_exec(db, "COMMIT;", NULL, NULL, &err) != SQLITE_OK ) {
		fprintf(stderr, "failed to commit transaction : %s\n", err);
		sqlite3_finalize(insert_param);
		exit(EX_SOFTWARE);
	}
}

int main(int argc, const char *argv[]) {
	if ( argc < 2 || get_verb(argv[1]) )
		usage(argv[0]);

	switch ( verb ) {
        	case init:
			get_db(argc, argv);
			init_db();
			break;
		
		case show:
			get_db(argc, argv);
			show_conf(argc, argv);
			break;

		case read:
			get_db(argc, argv);
			read_conf(argc, argv);
			break;

		default:
			usage(argv[0]);
			break;
	}
	return 0;
}
