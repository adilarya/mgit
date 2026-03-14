#include "mgit.h"
#include <arpa/inet.h> // For htonl/ntohl
#include <errno.h>
#include <zstd.h>

// --- Safe I/O Helpers ---
// These are essential for handling partial reads/writes in Pipes/Sockets.
ssize_t read_all(int fd, void* buf, size_t count)
{
    size_t total = 0;
    while (total < count) {
        ssize_t ret = read(fd, (char*)buf + total, count - total);
        if (ret == 0)
            break; // EOF
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total += ret;
    }
    return total;
}

ssize_t write_all(int fd, const void* buf, size_t count)
{
    size_t total = 0;
    while (total < count) {
        ssize_t ret = write(fd, (const char*)buf + total, count - total);
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total += ret;
    }
    return total;
}

// --- Serialization Helper ---
void* serialize_snapshot(Snapshot* snap, size_t* out_len)
{
    size_t total_size = sizeof(uint32_t) * 2 + 256;
    FileEntry* curr = snap->files;
    while (curr) {
        total_size += (sizeof(FileEntry) - sizeof(void*) * 2);
        if (curr->num_blocks > 0)
            total_size += (sizeof(BlockTable) * curr->num_blocks);
        curr = curr->next;
    }

    void* buf = malloc(total_size);
    void* ptr = buf;

    memcpy(ptr, &snap->snapshot_id, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, &snap->file_count, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, snap->message, 256);
    ptr += 256;

    curr = snap->files;
    while (curr) {
        size_t fixed_size = sizeof(FileEntry) - sizeof(void*) * 2;
        memcpy(ptr, curr, fixed_size);
        ptr += fixed_size;
        if (curr->num_blocks > 0) {
            size_t blocks_size = sizeof(BlockTable) * curr->num_blocks;
            memcpy(ptr, curr->chunks, blocks_size);
            ptr += blocks_size;
        }
        curr = curr->next;
    }

    *out_len = total_size;
    return buf;
}

void mgit_send(const char* id_str)
{
    // 1. Handshake Phase
    // TODO: Send the MAGIC_NUMBER (0x4D474954) to STDOUT.
    uint32_t id;
    if (id_str == NULL) {
        FILE* f = fopen(".mgit/HEAD", "r");
        if (f == NULL) {
            fprintf(stderr, "Error: Could not open .mgit/HEAD.\n");
            exit(1);
        }
        if (fscanf(f, "%u", &id) != 1) {
            fprintf(stderr, "Error: Could not read snapshot ID from .mgit/HEAD.\n");
            exit(1);
        }
        fclose(f);
    } else {
        id = atoi(id_str);
    }

    Snapshot* snap = load_snapshot_from_disk(id);
    if (snap == NULL) {
        fprintf(stderr, "Error: Snapshot %d not found.\n", id);
        exit(1);
    }
    uint32_t magic = htonl(MAGIC_NUMBER);
    write_all(STDOUT_FILENO, &magic, 4);


    // 2. Manifest Phase
    // TODO: Serialize the snapshot metadata and send its size followed by the buffer.
    size_t manifest_len;
    void* manifest_buf = serialize_snapshot(snap, &manifest_len);
    uint32_t net_len = htonl((uint32_t)manifest_len);
    write_all(STDOUT_FILENO, &net_len, 4);
    write_all(STDOUT_FILENO, manifest_buf, manifest_len);
    // 3. Payload Phase
    // TODO: Iterate through the files in the snapshot.
    // - If DISK MODE: Read the compressed chunks from ".mgit/data.bin" and write_all to STDOUT.
    // - If LIVE MODE: The chunks are already compressed in memory; send them directly.
    FILE* vault_file = fopen(".mgit/data.bin", "rb");
    if (vault_file == NULL) {
        fprintf(stderr, "Error opening vault: %s\n", strerror(errno));
        free(manifest_buf);
        free_file_list(snap->files);
        free(snap);
        exit(1);
    }
    FileEntry* curr = snap->files;
    while(curr != NULL){
        if (curr->is_directory == 0 && curr->num_blocks > 0){
            for (int i = 0; i < curr->num_blocks; i++){
               fseek(vault_file, curr->chunks[i].physical_offset, SEEK_SET);
                char* temp_buffer = malloc(curr->chunks[i].compressed_size);
                fread(temp_buffer, 1, curr->chunks[i].compressed_size, vault_file);
                write_all(STDOUT_FILENO, temp_buffer, curr->chunks[i].compressed_size);
                free(temp_buffer);
            }

        }
        curr = curr->next;
    }
    fclose(vault_file);
    free(manifest_buf);
    free_file_list(snap->files); 
    free(snap);
}

void mgit_receive(const char* dest_path)
{
    // 1. Setup
    // TODO: mkdir(dest_path) and mgit_init() inside it.

    char old_cwd[1024];
    if (getcwd(old_cwd, sizeof(old_cwd)) == NULL) {
        fprintf(stderr, "Error getting current working directory: %s\n", strerror(errno));
        return;
    }

    if (mkdir(dest_path, 0755) == -1) {
        if (errno != EEXIST) {
            fprintf(stderr, "Error creating directory '%s': %s\n", dest_path, strerror(errno));
            return;
        }
    }

    if (chdir(dest_path) == -1) {
        fprintf(stderr, "Error changing directory to '%s': %s\n", dest_path, strerror(errno));
        return;
    }

    mgit_init();

    uint32_t magic;
    if (read_all(STDIN_FILENO, &magic, 4) != 4)
        exit(1);
    if (ntohl(magic) != MAGIC_NUMBER) {
        fprintf(stderr, "Error: Invalid protocol\n");
        exit(1);
    }

    // 2. Handshake Phase
    // TODO: read_all from STDIN and verify the MAGIC_NUMBER.

    uint32_t net_len;
    if (read_all(STDIN_FILENO, &net_len, 4) != 4)
        exit(1);
    size_t manifest_len = ntohl(net_len);

    // 3. Manifest Reconstruction
    // TODO: Read the manifest size, allocate memory, and read the serialized data.
    // Reconstruct the linked list of FileEntries.

    void *buf = malloc(manifest_len);
    if (!buf) {
        fprintf(stderr, "Error allocating memory for manifest buffer.\n");
        exit(1);
    }

    if (read_all(STDIN_FILENO, buf, manifest_len) != manifest_len) {
        fprintf(stderr, "Error reading manifest data from STDIN.\n");
        free(buf);
        exit(1);
    }

    char *ptr = (char *)buf;

    Snapshot *snap = malloc(sizeof(Snapshot));
    if (!snap) {
        fprintf(stderr, "Error allocating memory for snapshot.\n");
        free(buf);
        exit(1);
    }

    memcpy(&snap->snapshot_id, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    memcpy(&snap->file_count, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    memcpy(snap->message, ptr, sizeof(snap->message));
    ptr += sizeof(snap->message);

    snap->files = NULL;

    FileEntry *head = NULL;
    FileEntry *tail = NULL;

    for (uint32_t i = 0; i < snap->file_count; i++) {
        FileEntry *node = malloc(sizeof(FileEntry));
        if (!node) {
            perror("malloc");
            free(buf);
            free_file_list(head);
            free(snap);
            exit(1);
        }

        memset(node, 0, sizeof(FileEntry));

        size_t fixed_size = sizeof(FileEntry) - sizeof(void*) * 2;
        memcpy(node, ptr, fixed_size);
        ptr += fixed_size;

        node->chunks = NULL;
        node->next = NULL;

        if (node->num_blocks > 0) {
            size_t blocks_size = sizeof(BlockTable) * node->num_blocks;
            node->chunks = malloc(blocks_size);
            if (!node->chunks) {
                perror("malloc");
                free(buf);
                free(node);
                free_file_list(head);
                free(snap);
                exit(1);
            }

            memcpy(node->chunks, ptr, blocks_size);
            ptr += blocks_size;
        }

        if (!head) {
            head = node;
            tail = node;
        } else {
            tail->next = node;
            tail = node;
        }
    }

    snap->files = head;
    free(buf);

    // 4. Processing Chunks (The Streaming OS Challenge)
    // TODO: Open ".mgit/data.bin" for appending.
    // For each file in the manifest:
    //   1. Open the file in the workspace for writing ("wb").
    //   2. While reading the compressed chunk from STDIN:
    //      - Write the raw compressed bytes into the local ".mgit/data.bin".
    //      - HINT: Don't forget to update physical_offset for the local vault!

    FILE *vault = fopen(".mgit/data.bin", "ab");
    if (!vault) {
        perror("fopen");
        free_file_list(snap->files);
        free(snap);
        exit(1);
    }

    FileEntry *curr = snap->files;
    char buffer[8192];

    while (curr) {
        if (!curr->is_directory && curr->num_blocks > 0 && curr->chunks != NULL) {
            uint32_t bytes_remaining = curr->chunks[0].compressed_size;

            if (fseek(vault, 0, SEEK_END) != 0) {
                perror("fseek");
                fclose(vault);
                free_file_list(snap->files);
                free(snap);
                exit(1);
            }

            long local_offset = ftell(vault);
            if (local_offset < 0) {
                perror("ftell");
                fclose(vault);
                free_file_list(snap->files);
                free(snap);
                exit(1);
            }

            curr->chunks[0].physical_offset = (uint64_t)local_offset;

            while (bytes_remaining > 0) {
                size_t want = (bytes_remaining < sizeof(buffer)) ? bytes_remaining : sizeof(buffer);

                ssize_t got = read_all(STDIN_FILENO, buffer, want);
                if (got != (ssize_t)want) {
                    fprintf(stderr, "Error: Incomplete payload stream\n");
                    fclose(vault);
                    free_file_list(snap->files);
                    free(snap);
                    exit(1);
                }

                if (fwrite(buffer, 1, want, vault) != want) {
                    perror("fwrite");
                    fclose(vault);
                    free_file_list(snap->files);
                    free(snap);
                    exit(1);
                }

                bytes_remaining -= (uint32_t)want;
            }
        }

        curr = curr->next;
    }

    fclose(vault);

    // 5. Cleanup
    // TODO: Save the new snapshot to disk and update HEAD.

    uint32_t local_head = get_current_head();
    uint32_t new_local_id = local_head + 1;

    /* Receiver assigns the incoming snapshot the next local ID */
    snap->snapshot_id = new_local_id;

    /* Save snapshot manifest locally, then update HEAD */
    store_snapshot_to_disk(snap);
    update_head(new_local_id);

    /* Free reconstructed snapshot */
    free_file_list(snap->files);
    free(snap);

    if (chdir(old_cwd) == -1) {
        fprintf(stderr, "Error changing back to original directory '%s': %s\n", old_cwd, strerror(errno));
        return;
    }
}
