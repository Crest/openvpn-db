CFLAGS+=-std=c99 -Wall -pedantic -D_WITH_GETLINE -I/usr/local/include
LDFLAGS+=-L/usr/local/lib -larchive -lsqlite3
CC=clang

all: openvpn-db

clean:
	rm -f openvpn-db

openvpn-db: openvpn-db.c
	$(CC) $(CFLAGS) -o openvpn-db openvpn-db.c $(LDFLAGS)
