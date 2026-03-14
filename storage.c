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
    
    block->size = (uint32_t)bytes_read_total;
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
    return NULL;
}

void chunks_recycle(uint32_t target_id)
{
    // TODO: Garbage Collection (The Vacuum)
    // 1. Load the oldest snapshot (target_id) and the newest snapshot (HEAD).
    // 2. Iterate through the oldest snapshot's files.
    // 3. If a chunk's physical_offset is NOT being used by ANY file in the HEAD snapshot,
    //    it is "stalled". Zero out those specific bytes in `data.bin`.
}

void mgit_snapshot(const char* msg)
{
    // TODO: 1. Get current HEAD ID and calculate next_id. Load previous files for crawling.
    // TODO: 2. Call build_file_list_bfs() to get the new directory state.

    // TODO: 3. Iterate through the new file list.
    // - If a file has data (chunks) but its size is 0, it needs to be written to the vault.
    // - CRITICAL: Check for Hard Links! If another file in the *current* list with the same
    //   inode was already written to the vault, copy its offset and size. DO NOT write twice!
    // - Call write_blob_to_vault() for new files.

    // TODO: 4. Call store_snapshot_to_disk() and update_head().
    // TODO: 5. Free memory.
    // TODO: 6. Enforce MAX_SNAPSHOT_HISTORY (5). If exceeded, call chunks_recycle()
    //          and delete the oldest manifest file using remove().
}
