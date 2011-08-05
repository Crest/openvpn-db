#include <stdio.h>
#include <stdlib.h>
#include <sysexits.h>

#include <sqlite3.h>

typedef enum {
	init, get, set
} verb_t;

verb_t verb = 0;
const char *db_path = NULL;
sqlite3 *db = NULL;


void usage(const char *name) {
	fprintf(stderr, "usage: %s init <DB>\n", name);
	exit(EX_USAGE);
}

int get_verb(const char *name) {
	if (!strcmp(name, "init")) {
		verb = init;
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

int main(int argc, const char *argv[]) {
	const char *name = argv[0];

	if ( argc < 2 || get_verb(argv[1]) )
		usage(name);

	switch ( verb ) {
        	case init:
			get_db(argc, argv);
			init_db();
			break;
		default:
			usage(name);
			break;
	}
	return 0;
}
