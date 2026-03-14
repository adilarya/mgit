#include "mgit.h"
#include <errno.h>

// Helper: Check if a path exists in the target snapshot
int path_in_snapshot(Snapshot* snap, const char* path)
{
    if (snap == NULL || path == NULL) {
        return 0;
    }
    // TODO: Iterate over snap->files and return 1 if the path matches, 0 otherwise.
    FileEntry* curr = snap->files;
    while(curr != NULL){
        if(strcmp(curr->path, path) == 0){
            return 1;
        }
        curr = curr->next;
    }
    return 0;
}

// Helper: Reverse the linked list
FileEntry* reverse_list(FileEntry* head)
{
    // TODO: Standard linked list reversal.
    // Why do we need this? Because BFS gives us Root -> Children.
    // To safely delete directories, we need to process Children -> Root.
    FileEntry* prev = NULL;
    FileEntry* curr = head;
    FileEntry* next = NULL;
    while (curr != NULL) {
        next = curr->next;
        curr->next = prev;
        prev = curr;
        curr = next;
    }
    //prev is the new head because list gto revsersed 
    return prev;
}

void mgit_restore(const char* id_str)
{
    if (!id_str)
        return;
    uint32_t id = atoi(id_str);

    Snapshot* target_snap = load_snapshot_from_disk(id);
    if (!target_snap) {
        fprintf(stderr, "Error: Snapshot %d not found.\n", id);
        exit(1);
    }

    // --- PHASE 1: SANITIZATION (The Purge) ---
    // Remove files that exist currently but NOT in the target snapshot.
    FileEntry* current_files = build_file_list_bfs(".", NULL);
    FileEntry* reversed = reverse_list(current_files);

    // TODO: Iterate through 'reversed'.
    // If a file/dir exists on disk (but is not ".") AND is not in target_snap:
    //   - Use rmdir() if it's a directory.
    //   - Use unlink() if it's a file.
    FileEntry* curr = reversed;
    while(curr != NULL){
        if (strcmp(curr->path, ".") != 0 && path_in_snapshot(target_snap, curr->path) == 0) {
            if (curr->is_directory == 1) {
                if (rmdir(curr->path) == -1) {
                    // fprintf(stderr, "Error removing directory '%s': %s\n", curr->path, strerror(errno));
                }
            } else {
                if (unlink(curr->path) == -1) {
                    // fprintf(stderr, "Error removing file '%s': %s\n", curr->path, strerror(errno));
                }
            }
        }
        curr = curr->next; 
    }

    free_file_list(reversed);

    // --- PHASE 2: RECONSTRUCTION & INTEGRITY ---
    // TODO: Iterate through target_snap->files.

    // HINT:
    // 1. If it's a directory (and not "."), recreate it using mkdir() with 0755.
    // 2. If it's a file, open it for writing ("wb").
    // 3. For each block in curr->chunks, call read_blob_from_vault() to write the data back to disk.

    FileEntry* target_curr = target_snap->files;
    while (target_curr != NULL) {
        if (strcmp(target_curr->path, ".") != 0) {
            if (target_curr->is_directory == 1) {
                if (mkdir(target_curr->path, 0755) == -1) {
                    if (errno != EEXIST) { 
                            // fprintf(stderr, "Error creating directory '%s': %s\n", target_curr->path, strerror(errno));
                    }                
                }
            } else {
                struct stat st;
                // special memory check if the file already exists and has the correct size, we can skip writing it
                if (stat(target_curr->path, &st) == 0) {
                    if (st.st_size == target_curr->size && st.st_mtime == target_curr->mtime) {
                        // the file is already how we need it
                        target_curr = target_curr->next;
                        continue;
                    }
                }
                FILE* out_file = fopen(target_curr->path, "wb");
                if (out_file == NULL) {
                    // fprintf(stderr, "Error creating file '%s': %s\n", target_curr->path, strerror(errno));
                    target_curr = target_curr->next;
                    continue;
                }
                for (int i = 0; i < target_curr->num_blocks; i++) {
                    read_blob_from_vault(
                            target_curr->chunks[i].physical_offset, 
                            target_curr->chunks[i].compressed_size, 
                            fileno(out_file)
                        );                
                    }
                fclose(out_file);
                // --- INTEGRITY CHECK (Corruption Detection) ---
                // TODO: After writing a file, compute its hash using your compute_hash() function.
                // Compare the newly computed hash with the curr->checksum stored in the snapshot.
                // If they do not match (memcmp), print a corruption error, unlink() the bad file,
                // and exit(1) to abort the restore.

                uint8_t computed_checksum[32];
                compute_hash(target_curr->path, computed_checksum);
                if (memcmp(computed_checksum, target_curr->checksum, 32) != 0) {
                    fprintf(stderr, "Error: Checksum mismatch for file '%s'. File may be corrupted.\n", target_curr->path);
                    if (unlink(target_curr->path) == -1) {
                        fprintf(stderr, "Error removing corrupted file '%s': %s\n", target_curr->path, strerror(errno));
                    }
                    exit(1);
                }
            }
        }
        target_curr = target_curr->next;
    }



    

    // Cleanup
    free_file_list(target_snap->files);
    free(target_snap);
}
