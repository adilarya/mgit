#include "mgit.h"
#include <arpa/inet.h>
#include <errno.h>
#include <zstd.h>

ssize_t read_all(int fd, void* buf, size_t count)
{
    size_t total = 0;
    while (total < count) {
        ssize_t ret = read(fd, (char*)buf + total, count - total);
        if (ret == 0)
            break;
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total += (size_t)ret;
    }
    return (ssize_t)total;
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
        total += (size_t)ret;
    }
    return (ssize_t)total;
}

void* serialize_snapshot(Snapshot* snap, size_t* out_len)
{
    size_t total_size = sizeof(uint32_t) * 2 + 256;
    FileEntry* curr = snap->files;

    while (curr) {
        total_size += sizeof(FileEntry) - sizeof(void*) * 2;
        if (curr->num_blocks > 0)
            total_size += sizeof(BlockTable) * (size_t)curr->num_blocks;
        curr = curr->next;
    }

    void* buf = malloc(total_size);
    char* ptr;

    if (buf == NULL) {
        fprintf(stderr, "Error: Failed to allocate manifest buffer.\n");
        exit(1);
    }

    ptr = (char*)buf;

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
            size_t blocks_size = sizeof(BlockTable) * (size_t)curr->num_blocks;
            memcpy(ptr, curr->chunks, blocks_size);
            ptr += blocks_size;
        }

        curr = curr->next;
    }

    *out_len = total_size;
    return buf;
}

static uint32_t count_files(FileEntry* head)
{
    uint32_t count = 0;
    while (head != NULL) {
        count++;
        head = head->next;
    }
    return count;
}

static void send_saved_snapshot(uint32_t id)
{
    Snapshot* snap = load_snapshot_from_disk(id);
    FileEntry* curr;
    uint32_t magic;
    size_t manifest_len;
    void* manifest_buf;
    uint32_t net_len;
    FILE* vault_file;
    char buffer[8192];

    if (snap == NULL) {
        fprintf(stderr, "Error: Snapshot %u not found.\n", id);
        exit(1);
    }

    magic = htonl(MAGIC_NUMBER);
    if (write_all(STDOUT_FILENO, &magic, 4) != 4) {
        fprintf(stderr, "Error: Failed to send handshake.\n");
        free_file_list(snap->files);
        free(snap);
        exit(1);
    }

    manifest_buf = serialize_snapshot(snap, &manifest_len);
    net_len = htonl((uint32_t)manifest_len);

    if (write_all(STDOUT_FILENO, &net_len, 4) != 4 ||
        write_all(STDOUT_FILENO, manifest_buf, manifest_len) != (ssize_t)manifest_len) {
        fprintf(stderr, "Error: Failed to send manifest.\n");
        free(manifest_buf);
        free_file_list(snap->files);
        free(snap);
        exit(1);
    }

    vault_file = fopen(".mgit/data.bin", "rb");
    if (vault_file == NULL) {
        fprintf(stderr, "Error: Could not open .mgit/data.bin.\n");
        free(manifest_buf);
        free_file_list(snap->files);
        free(snap);
        exit(1);
    }

    curr = snap->files;
    while (curr != NULL) {
        if (!curr->is_directory && curr->num_blocks > 0) {
            int i;
            for (i = 0; i < curr->num_blocks; i++) {
                uint32_t remaining;

                if (fseek(vault_file, (long)curr->chunks[i].physical_offset, SEEK_SET) != 0) {
                    fprintf(stderr, "Error: Failed to seek vault.\n");
                    fclose(vault_file);
                    free(manifest_buf);
                    free_file_list(snap->files);
                    free(snap);
                    exit(1);
                }

                remaining = curr->chunks[i].compressed_size;
                while (remaining > 0) {
                    size_t want = remaining < sizeof(buffer) ? remaining : sizeof(buffer);
                    size_t got = fread(buffer, 1, want, vault_file);

                    if (got != want) {
                        fprintf(stderr, "Error: Failed to read vault payload.\n");
                        fclose(vault_file);
                        free(manifest_buf);
                        free_file_list(snap->files);
                        free(snap);
                        exit(1);
                    }

                    if (write_all(STDOUT_FILENO, buffer, got) != (ssize_t)got) {
                        fprintf(stderr, "Error: Failed to write payload.\n");
                        fclose(vault_file);
                        free(manifest_buf);
                        free_file_list(snap->files);
                        free(snap);
                        exit(1);
                    }

                    remaining -= (uint32_t)got;
                }
            }
        }
        curr = curr->next;
    }

    fclose(vault_file);
    free(manifest_buf);
    free_file_list(snap->files);
    free(snap);
}

static void send_live_snapshot(void)
{
    FileEntry* files = build_file_list_bfs(".", NULL);
    Snapshot snap;
    FileEntry* curr;
    uint32_t magic;
    size_t manifest_len;
    void* manifest_buf;
    uint32_t net_len;
    char buffer[8192];

    if (files == NULL) {
        fprintf(stderr, "Error: Failed to build live file list.\n");
        exit(1);
    }

    curr = files;
    while (curr != NULL) {
        if (!curr->is_directory && curr->num_blocks > 0 && curr->chunks != NULL) {
            int i;
            for (i = 0; i < curr->num_blocks; i++) {
                curr->chunks[i].physical_offset = 0;
                curr->chunks[i].compressed_size = (uint32_t)curr->size;
            }
        }
        curr = curr->next;
    }

    memset(&snap, 0, sizeof(Snapshot));
    snap.snapshot_id = 0;
    snap.file_count = count_files(files);
    strncpy(snap.message, "LIVE", sizeof(snap.message) - 1);
    snap.files = files;

    magic = htonl(MAGIC_NUMBER);
    if (write_all(STDOUT_FILENO, &magic, 4) != 4) {
        fprintf(stderr, "Error: Failed to send handshake.\n");
        free_file_list(files);
        exit(1);
    }

    manifest_buf = serialize_snapshot(&snap, &manifest_len);
    net_len = htonl((uint32_t)manifest_len);

    if (write_all(STDOUT_FILENO, &net_len, 4) != 4 ||
        write_all(STDOUT_FILENO, manifest_buf, manifest_len) != (ssize_t)manifest_len) {
        fprintf(stderr, "Error: Failed to send manifest.\n");
        free(manifest_buf);
        free_file_list(files);
        exit(1);
    }

    curr = files;
    while (curr != NULL) {
        if (!curr->is_directory && curr->num_blocks > 0) {
            FILE* in = fopen(curr->path, "rb");
            if (in == NULL) {
                fprintf(stderr, "Error: Failed to open '%s' for live send.\n", curr->path);
                free(manifest_buf);
                free_file_list(files);
                exit(1);
            }

            for (;;) {
                size_t got = fread(buffer, 1, sizeof(buffer), in);

                if (got > 0 && write_all(STDOUT_FILENO, buffer, got) != (ssize_t)got) {
                    fprintf(stderr, "Error: Failed to stream file payload.\n");
                    fclose(in);
                    free(manifest_buf);
                    free_file_list(files);
                    exit(1);
                }

                if (got < sizeof(buffer)) {
                    if (ferror(in)) {
                        fprintf(stderr, "Error: Failed while reading '%s'.\n", curr->path);
                        fclose(in);
                        free(manifest_buf);
                        free_file_list(files);
                        exit(1);
                    }
                    break;
                }
            }

            fclose(in);
        }
        curr = curr->next;
    }

    free(manifest_buf);
    free_file_list(files);
}

void mgit_send(const char* id_str)
{
    if (id_str == NULL) {
        send_live_snapshot();
        return;
    }

    send_saved_snapshot((uint32_t)atoi(id_str));
}

void mgit_receive(const char* dest_path)
{
    char old_cwd[1024];
    uint32_t magic;
    uint32_t net_len;
    size_t manifest_len;
    void* buf;
    char* ptr;
    Snapshot* snap;
    FileEntry* head = NULL;
    FileEntry* tail = NULL;
    FILE* vault;
    FileEntry* curr;
    char buffer[8192];

    if (getcwd(old_cwd, sizeof(old_cwd)) == NULL) {
        fprintf(stderr, "Error: Failed to get current working directory.\n");
        exit(1);
    }

    if (mkdir(dest_path, 0755) == -1 && errno != EEXIST) {
        fprintf(stderr, "Error: Failed to create destination '%s'.\n", dest_path);
        exit(1);
    }

    if (chdir(dest_path) == -1) {
        fprintf(stderr, "Error: Failed to enter destination '%s'.\n", dest_path);
        exit(1);
    }

    mgit_init();

    if (read_all(STDIN_FILENO, &magic, 4) != 4) {
        fprintf(stderr, "Error: Failed to read protocol handshake.\n");
        chdir(old_cwd);
        exit(1);
    }

    if (ntohl(magic) != MAGIC_NUMBER) {
        fprintf(stderr, "Error: Invalid protocol handshake.\n");
        chdir(old_cwd);
        exit(1);
    }

    if (read_all(STDIN_FILENO, &net_len, 4) != 4) {
        fprintf(stderr, "Error: Failed to read manifest length.\n");
        chdir(old_cwd);
        exit(1);
    }

    manifest_len = ntohl(net_len);
    if (manifest_len < sizeof(uint32_t) * 2 + 256) {
        fprintf(stderr, "Error: Invalid manifest length.\n");
        chdir(old_cwd);
        exit(1);
    }

    buf = malloc(manifest_len);
    if (buf == NULL) {
        fprintf(stderr, "Error: Failed to allocate manifest buffer.\n");
        chdir(old_cwd);
        exit(1);
    }

    if (read_all(STDIN_FILENO, buf, manifest_len) != (ssize_t)manifest_len) {
        fprintf(stderr, "Error: Incomplete manifest data.\n");
        free(buf);
        chdir(old_cwd);
        exit(1);
    }

    ptr = (char*)buf;
    snap = malloc(sizeof(Snapshot));
    if (snap == NULL) {
        fprintf(stderr, "Error: Failed to allocate snapshot.\n");
        free(buf);
        chdir(old_cwd);
        exit(1);
    }

    memset(snap, 0, sizeof(Snapshot));

    memcpy(&snap->snapshot_id, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(&snap->file_count, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(snap->message, ptr, sizeof(snap->message));
    ptr += sizeof(snap->message);

    {
        uint32_t i;
        for (i = 0; i < snap->file_count; i++) {
            FileEntry* node = malloc(sizeof(FileEntry));
            size_t fixed_size;

            if (node == NULL) {
                fprintf(stderr, "Error: Failed to allocate FileEntry.\n");
                free(buf);
                free_file_list(head);
                free(snap);
                chdir(old_cwd);
                exit(1);
            }

            memset(node, 0, sizeof(FileEntry));
            fixed_size = sizeof(FileEntry) - sizeof(void*) * 2;
            memcpy(node, ptr, fixed_size);
            ptr += fixed_size;

            node->chunks = NULL;
            node->next = NULL;

            if (node->num_blocks > 0) {
                size_t blocks_size = sizeof(BlockTable) * (size_t)node->num_blocks;
                node->chunks = malloc(blocks_size);

                if (node->chunks == NULL) {
                    fprintf(stderr, "Error: Failed to allocate block table.\n");
                    free(buf);
                    free(node);
                    free_file_list(head);
                    free(snap);
                    chdir(old_cwd);
                    exit(1);
                }

                memcpy(node->chunks, ptr, blocks_size);
                ptr += blocks_size;
            }

            if (head == NULL) {
                head = node;
                tail = node;
            } else {
                tail->next = node;
                tail = node;
            }
        }
    }

    snap->files = head;
    free(buf);

    vault = fopen(".mgit/data.bin", "ab");
    if (vault == NULL) {
        fprintf(stderr, "Error: Failed to open local vault.\n");
        free_file_list(snap->files);
        free(snap);
        chdir(old_cwd);
        exit(1);
    }

    curr = snap->files;
    while (curr != NULL) {
        if (!curr->is_directory && curr->num_blocks > 0 && curr->chunks != NULL) {
            int i;
            for (i = 0; i < curr->num_blocks; i++) {
                uint32_t bytes_remaining = curr->chunks[i].compressed_size;

                if (fseek(vault, 0, SEEK_END) != 0) {
                    fprintf(stderr, "Error: Failed to seek local vault.\n");
                    fclose(vault);
                    free_file_list(snap->files);
                    free(snap);
                    chdir(old_cwd);
                    exit(1);
                }

                {
                    long local_offset = ftell(vault);
                    if (local_offset < 0) {
                        fprintf(stderr, "Error: Failed to get local vault offset.\n");
                        fclose(vault);
                        free_file_list(snap->files);
                        free(snap);
                        chdir(old_cwd);
                        exit(1);
                    }
                    curr->chunks[i].physical_offset = (uint64_t)local_offset;
                }

                while (bytes_remaining > 0) {
                    size_t want = bytes_remaining < sizeof(buffer) ? bytes_remaining : sizeof(buffer);
                    ssize_t got = read_all(STDIN_FILENO, buffer, want);

                    if (got != (ssize_t)want) {
                        fprintf(stderr, "Error: Incomplete payload stream.\n");
                        fclose(vault);
                        free_file_list(snap->files);
                        free(snap);
                        chdir(old_cwd);
                        exit(1);
                    }

                    if (fwrite(buffer, 1, want, vault) != want) {
                        fprintf(stderr, "Error: Failed to write payload to local vault.\n");
                        fclose(vault);
                        free_file_list(snap->files);
                        free(snap);
                        chdir(old_cwd);
                        exit(1);
                    }

                    bytes_remaining -= (uint32_t)want;
                }
            }
        }

        curr = curr->next;
    }

    fclose(vault);

    snap->snapshot_id = get_current_head() + 1;
    store_snapshot_to_disk(snap);
    update_head(snap->snapshot_id);

    {
        char id_buf[32];
        snprintf(id_buf, sizeof(id_buf), "%u", snap->snapshot_id);

        free_file_list(snap->files);
        free(snap);

        mgit_restore(id_buf);
    }

    if (chdir(old_cwd) == -1) {
        fprintf(stderr, "Error: Failed to restore original working directory.\n");
        exit(1);
    }
}