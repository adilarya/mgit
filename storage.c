#include "mgit.h"
#include <errno.h>
#include <zstd.h>

// --- Helper Functions ---
uint32_t get_current_head()
{
    // TODO: Read the integer from ".mgit/HEAD" and return it. Return 0 if it fails.
    FILE* head_file = fopen(".mgit/HEAD", "r");
    if (head_file == NULL) {
        return 0;
    }
    uint32_t head_id;
    if (fscanf(head_file, "%u", &head_id) != 1) {
        fprintf(stderr, "Error: %s\n", strerror(errno));
        fclose(head_file);
        return 0;
    }
    fclose(head_file);
    return head_id;
}

void update_head(uint32_t new_id)
{
    // TODO: Overwrite ".mgit/HEAD" with the new_id.
    FILE* head_file = fopen(".mgit/HEAD", "w");
    if (head_file == NULL) {
        fprintf(stderr, "Error: %s\n", strerror(errno));
        return;
    }
    if (fprintf(head_file, "%u", new_id) < 0) {
        fprintf(stderr, "Error: %s\n", strerror(errno));
    }
    fclose(head_file);

}

// --- Blob Storage (Raw) ---
void write_blob_to_vault(const char* filepath, BlockTable* block)
{
    // TODO: Open `filepath` for reading (rb).
    // TODO: Open `.mgit/data.bin` for APPENDING (ab).
    // TODO: Use ftell() to record the current end of the vault into block->physical_offset.
    // TODO: Read the file bytes and write them into the vault. Update block->size.

    if (block == NULL) {
        fprintf(stderr, "Error: BlockTable pointer is NULL.\n");
        return;
    }

    FILE* in_file = fopen(filepath, "rb");
    if (in_file == NULL) {
        fprintf(stderr, "Error opening file '%s': %s\n", filepath, strerror(errno));
        return;
    }

    FILE* vault_file = fopen(".mgit/data.bin", "ab");
    if (vault_file == NULL) {
        fprintf(stderr, "Error opening vault: %s\n", strerror(errno));
        fclose(in_file);
        return;
    }

    // Move to the end of the vault to get the current offset
    if (fseek(vault_file, 0, SEEK_END) != 0) {
        fprintf(stderr, "Error seeking in vault: %s\n", strerror(errno));
        fclose(in_file);
        fclose(vault_file);
        return;
    }

    long offset = ftell(vault_file);
    if (offset == -1) {
        fprintf(stderr, "Error getting vault offset: %s\n", strerror(errno));
        fclose(in_file);
        fclose(vault_file);
        return;
    }

    // Read the file bytes and write them into the vault. Update block->size.
    uint8_t buffer[4096];
    size_t bytes_read;
    size_t bytes_read_total = 0;
    while ((bytes_read = fread(buffer, 1, sizeof(buffer), in_file)) > 0) {
        if (fwrite(buffer, 1, bytes_read, vault_file) != bytes_read) {
            fprintf(stderr, "Error writing to vault: %s\n", strerror(errno));
            fclose(in_file);
            fclose(vault_file);
            return;
        }
        bytes_read_total += bytes_read;
    }

    if (ferror(in_file)) {
        fprintf(stderr, "Error reading file '%s': %s\n", filepath, strerror(errno));
        fclose(in_file);
        fclose(vault_file);
        return;
    }
    
    block->compressed_size = (uint32_t)bytes_read_total;
    block->physical_offset = (uint64_t)offset;

    fclose(in_file);
    fclose(vault_file);
}

void read_blob_from_vault(uint64_t offset, uint32_t size, int out_fd)
{
    // TODO: Open the vault, fseek() to the physical_offset.
    // TODO: Read `size` bytes and write them to `out_fd` using the write_all() helper.

    if (size == 0) {
        return; // Nothing to read
    }

    FILE* vault_file = fopen(".mgit/data.bin", "rb");
    if (vault_file == NULL) {
        fprintf(stderr, "Error opening vault: %s\n", strerror(errno));
        return;
    }

    if (fseek(vault_file, (long)offset, SEEK_SET) != 0) {
        fprintf(stderr, "Error seeking in vault: %s\n", strerror(errno));
        fclose(vault_file);
        return;
    }

    uint8_t buffer[4096];
    uint32_t bytes_remaining = size;
    while (bytes_remaining > 0) {
        size_t chunk_size = (bytes_remaining < sizeof(buffer)) ? bytes_remaining : sizeof(buffer);
        size_t bytes_read = fread(buffer, 1, chunk_size, vault_file);
        if (bytes_read == 0) {
            if (feof(vault_file)) {
                fprintf(stderr, "Unexpected end of vault file.\n");
            } else {
                fprintf(stderr, "Error reading from vault: %s\n", strerror(errno));
            }
            fclose(vault_file);
            return;
        }
        if (write_all(out_fd, buffer, bytes_read) < 0) {
            fprintf(stderr, "Error writing to output fd: %s\n", strerror(errno));
            fclose(vault_file);
            return;
        }
        bytes_remaining -= (uint32_t)bytes_read;
    }

    if (bytes_remaining != 0) {
        fprintf(stderr, "Error: Expected to read %u bytes but %u bytes remain.\n", size, bytes_remaining);
    }

    fclose(vault_file);
}

// --- Snapshot Management ---
void store_snapshot_to_disk(Snapshot* snap)

{
    // TODO: Serialize the Snapshot struct and its linked list of FileEntry/BlockTables

    // into a binary file inside `.mgit/snapshots/snap_XXX.bin`.
    char filename[256];
    snprintf(filename, sizeof(filename), ".mgit/snapshots/snap_%03u.bin", snap->snapshot_id);   
    FILE* snap_file = fopen(filename, "wb");
    if (snap_file == NULL) {
        fprintf(stderr, "Error opening snapshot file '%s': %s\n", filename, strerror(errno));
        return;
    }

    fwrite(snap, sizeof(Snapshot), 1, snap_file);
    FileEntry* curr = snap->files;
    while(curr != NULL){
        fwrite(curr, sizeof(FileEntry), 1, snap_file);
        if(curr->is_directory == 0 && curr->num_blocks >0){
            
            fwrite(curr->chunks, sizeof(BlockTable), curr->num_blocks, snap_file);
            
        }
        curr = curr->next;
    }
    
    fclose(snap_file);


}


Snapshot* load_snapshot_from_disk(uint32_t id)
{
    // TODO: Read a `snap_XXX.bin` file and reconstruct the Snapshot struct
    // and its FileEntry linked list in heap memory.

    char filename[256];
    snprintf(filename, sizeof(filename), ".mgit/snapshots/snap_%03u.bin", id);
    FILE* snap_file = fopen(filename, "rb");
    if (snap_file == NULL) {
        fprintf(stderr, "Error opening snapshot file '%s': %s\n", filename, strerror(errno));
        return NULL;
    }

    Snapshot* snap = malloc(sizeof(Snapshot));
    if (snap == NULL) {
        fprintf(stderr, "Error allocating memory for snapshot.\n");
        fclose(snap_file);
        
        return NULL;
    }

    if (fread(snap, sizeof(Snapshot), 1, snap_file) != 1) {
        fprintf(stderr, "Error reading snapshot header from file '%s'.\n", filename);
        free(snap);
        fclose(snap_file);
        return NULL;
    }
    FileEntry* head = NULL;
    FileEntry* tail = NULL;
    snap->files = NULL;
    for (uint32_t i = 0; i < snap->file_count; i++) {
        FileEntry* entry = malloc(sizeof(FileEntry));
        if (entry == NULL) {
            fprintf(stderr, "Error allocating memory for FileEntry.\n");
            free(snap);
            fclose(snap_file);
            free_file_list(head);
            return NULL;
        }
        if (fread(entry, sizeof(FileEntry), 1, snap_file) != 1) {
            fprintf(stderr, "Error reading FileEntry from file '%s'.\n", filename);
            free(entry);
            free(snap);
            fclose(snap_file);
            free_file_list(head);
            return NULL;
        }
        entry->next = NULL;
        if (entry->is_directory == 0 && entry->num_blocks > 0) {
        entry->chunks = malloc(sizeof(BlockTable) * entry->num_blocks);
        if (fread(entry->chunks, sizeof(BlockTable), entry->num_blocks, snap_file) != entry->num_blocks) {
            fprintf(stderr, "Error reading BlockTable from file '%s'.\n", filename);
            free(entry->chunks);
            free(entry);
            free(snap);
            fclose(snap_file);
            free_file_list(head);
            return NULL;
        }
        } else {
            entry->chunks = NULL;
        }
        if (head == NULL) {
        head = entry;
        tail = entry;
        } else {
            tail->next = entry;
            tail = entry;
        }
        
    }
    snap->files = head;
    fclose(snap_file);
    return snap;
}

int is_offset_used(Snapshot* snap, uint64_t target_offset) {
    if (snap == NULL || snap->files == NULL) {
        return 0; 
    }
    FileEntry* curr = snap->files;
    while (curr != NULL) {       
        if (curr->is_directory == 0 && curr->num_blocks > 0 && curr->chunks != NULL) {          
            for (uint32_t i = 0; i < curr->num_blocks; i++) {
                if (curr->chunks[i].physical_offset == target_offset) {
                    return 1; 
                }
            }
        }
        curr = curr->next;
    }
    return 0;
}
void chunks_recycle(uint32_t target_id)
{
    // TODO: Garbage Collection (The Vacuum)
    // 1. Load the oldest snapshot (target_id) and the newest snapshot (HEAD).
    // 2. Iterate through the oldest snapshot's files.
    // 3. If a chunk's physical_offset is NOT being used by ANY file in the HEAD snapshot,
    //    it is "stalled". Zero out those specific bytes in `data.bin`.
    Snapshot* old_snap = load_snapshot_from_disk(target_id);
    if (old_snap == NULL) {
        fprintf(stderr, "Error: Old snapshot %u not found for recycling.\n", target_id);
        return;
    }
    Snapshot* head_snap = load_snapshot_from_disk(get_current_head());
    if (head_snap == NULL) {
        fprintf(stderr, "Error: Current HEAD snapshot not found for recycling.\n");
        free_file_list(old_snap->files);
        free(old_snap);
        return;
    }
    FILE* vault_file = fopen(".mgit/data.bin", "r+b");
    if (vault_file == NULL) {
        fprintf(stderr, "Error opening vault for recycling: %s\n", strerror(errno));
        free_file_list(old_snap->files);
        free(old_snap);
        free_file_list(head_snap->files);
        free(head_snap);
        return;
    }
    FileEntry* old_curr = old_snap->files;
    while (old_curr != NULL) {
        if (old_curr->is_directory == 0 && old_curr->num_blocks > 0) {
            
            for (uint32_t i = 0; i < old_curr->num_blocks; i++) {
                
                if (is_offset_used(head_snap, old_curr->chunks[i].physical_offset) == 0) {
                    
                    if (fseek(vault_file, (long)old_curr->chunks[i].physical_offset, SEEK_SET) != 0) {
                        fprintf(stderr, "Error seeking in vault: %s\n", strerror(errno));
                        break;
                    }
                    
                    uint8_t zero_buffer[4096] = {0};
                    size_t bytes_to_zero = old_curr->chunks[i].compressed_size; // Note chunks[i]
                    
                    while (bytes_to_zero > 0) {
                        size_t chunk_size = (bytes_to_zero < sizeof(zero_buffer)) ? bytes_to_zero : sizeof(zero_buffer);
                        if (fwrite(zero_buffer, 1, chunk_size, vault_file) != chunk_size) {
                            fprintf(stderr, "Error writing zeros to vault: %s\n", strerror(errno));
                            break;
                        }
                        bytes_to_zero -= chunk_size;
                    }
                }
            }
        }
        old_curr = old_curr->next;
    }    
    fclose(vault_file);
    free_file_list(old_snap->files);
    free(old_snap);
    free_file_list(head_snap->files);
    free(head_snap);
}

void mgit_snapshot(const char* msg)
{
    // TODO: 1. Get current HEAD ID and calculate next_id. Load previous files for crawling.

    int current_head = (int)get_current_head();
    int next_id = current_head + 1;
    Snapshot* prev_snap = load_snapshot_from_disk((uint32_t)current_head);
    FileEntry* prev_snap_files = (prev_snap != NULL) ? prev_snap->files : NULL;

    // TODO: 2. Call build_file_list_bfs() to get the new directory state.

    FileEntry* new_dir_state = build_file_list_bfs(".", prev_snap_files);
    if (new_dir_state == NULL) {
        fprintf(stderr, "Error: Failed to build file list.\n");
        return;
    }

    // TODO: 3. Iterate through the new file list.
    // - If a file has data (chunks) but its size is 0, it needs to be written to the vault.
    // - CRITICAL: Check for Hard Links! If another file in the *current* list with the same
    //   inode was already written to the vault, copy its offset and size. DO NOT write twice!
    // - Call write_blob_to_vault() for new files.

    FileEntry* current = new_dir_state;
    while (current != NULL) {
        if (current->chunks != NULL && current->chunks->compressed_size == 0) {
            // Check for hard links in the current list
            FileEntry* check = new_dir_state;
            int found_hard_link = 0;
            while (check != current) {
                if (check->inode == current->inode && check->chunks != NULL && check->chunks->compressed_size > 0) {
                    // Found a hard link, copy offset and size
                    current->chunks->physical_offset = check->chunks->physical_offset;
                    current->chunks->compressed_size = check->chunks->compressed_size;
                    found_hard_link = 1;
                    break;
                }
                check = check->next;
            }
            if (!found_hard_link) {
                // No hard link found, write to vault
                write_blob_to_vault(current->path, current->chunks);
            }
        }
        current = current->next;
    }

    // TODO: 4. Call store_snapshot_to_disk() and update_head().

    // getting file_count for the snapshot
    uint32_t file_count = 0;
    current = new_dir_state;
    while (current != NULL) {
        file_count++;
        current = current->next;
    }

    Snapshot new_snapshot;
    new_snapshot.snapshot_id = (uint32_t)next_id;
    new_snapshot.file_count = file_count;
    strncpy(new_snapshot.message, msg, sizeof(new_snapshot.message) - 1);
    new_snapshot.message[sizeof(new_snapshot.message) - 1] = '\0'; // Ensure null-termination
    new_snapshot.files = new_dir_state;
    
    store_snapshot_to_disk(&new_snapshot);
    update_head((uint32_t)next_id);

    // TODO: 5. Free memory.

    free_file_list(new_dir_state);
    if (prev_snap != NULL) {
        free_file_list(prev_snap->files);
        free(prev_snap);
    }

    // TODO: 6. Enforce MAX_SNAPSHOT_HISTORY (5). If exceeded, call chunks_recycle()
    //          and delete the oldest manifest file using remove().

    if (next_id > MAX_SNAPSHOT_HISTORY) {
        uint32_t target_id = (uint32_t)(next_id - MAX_SNAPSHOT_HISTORY);
        chunks_recycle(target_id);
        char old_snap_path[256];
        snprintf(old_snap_path, sizeof(old_snap_path), ".mgit/snapshots/snap_%03u.bin", target_id);
        if (remove(old_snap_path) != 0) {
            fprintf(stderr, "Error deleting old snapshot '%s': %s\n", old_snap_path, strerror(errno));
        }
    }

}
