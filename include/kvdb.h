#ifndef KVDB_H
#define KVDB_H

#include <stddef.h>

/* Opaque handle */
typedef struct kv_table kv_table_t;

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

/*
 * Elenco chiavi.
 * out_keys  -> array di stringhe allocato dinamicamente.
 * out_count -> numero di chiavi.
 * Il chiamante deve liberare con kv_keys_free().
 */
int kv_keys(kv_table_t *db, char ***out_keys, size_t *out_count);
void kv_keys_free(char **keys, size_t count);

#endif
