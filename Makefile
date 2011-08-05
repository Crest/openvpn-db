CFLAGS+=-D_POSIX_C_SOURCE=200809 -I/usr/local/include
LDFLAGS+=-L/usr/local/lib -lsqlite3
all: openvpn-db

clean:
	rm -f openvpn-db

openvpn-db: openvpn-db.c
	$(CC) $(CFLAGS) -o openvpn-db openvpn-db.c $(LDFLAGS)
