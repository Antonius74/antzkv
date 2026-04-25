#ifndef KVDB_H
#define KVDB_H

#include <stddef.h>
#include <stdint.h>

typedef struct kv_table kv_table_t;

typedef struct {
    uint64_t version;
    uint64_t wallclock;
    char origin[16];
} kv_meta_t;

/* Apertura DB. Se path != NULL carica o crea il file di persistenza. */
kv_table_t *kv_open(const char *path);

/* Chiusura DB. Salva su disco se path era fornito. */
void kv_close(kv_table_t *db);

/* Operazioni CRUD. Restituiscono 0 in caso di successo, -1 in caso di errore. */
int kv_set(kv_table_t *db, const char *key, const char *value);
char       *kv_get(kv_table_t *db, const char *key);  /* Ritorna stringa allocata; free() del chiamante */
int         kv_del(kv_table_t *db, const char *key);
int         kv_exists(kv_table_t *db, const char *key);

/* Salvataggio esplicito. Restituisce 0 in caso di successo. */
int kv_save(kv_table_t *db);

/* Elenco chiavi. */
int kv_keys(kv_table_t *db, char ***out_keys, size_t *out_count);
void kv_keys_free(char **keys, size_t count);

/* CRUD con metadata per replica cluster (Last-Write-Wins) */
int kv_set_meta(kv_table_t *db, const char *key, const char *value, const kv_meta_t *meta);
int kv_del_meta(kv_table_t *db, const char *key, const kv_meta_t *meta);
char *kv_get_meta(kv_table_t *db, const char *key, kv_meta_t *out_meta);

/* Prossima versione atomica */
uint64_t kv_next_version(kv_table_t *db);

/* Ultima versione conosciuta del db */
uint64_t kv_db_version(kv_table_t *db);

#endif
