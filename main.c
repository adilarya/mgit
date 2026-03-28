#include "mgit.h"
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

//Project made by Adil Arya and Eshwar Rajasekar

int main(int argc, char* argv[])
{
    // Basic routing logic is provided.
    if (argc < 2)
        return 1;

    if (strcmp(argv[1], "init") == 0) {
        mgit_init();
    } else if (strcmp(argv[1], "snapshot") == 0) {
        if (argc < 3)
            return 1;
        mgit_snapshot(argv[2]);
    } else if (strcmp(argv[1], "send") == 0) {
        mgit_send(argc > 2 ? argv[2] : NULL);
    } else if (strcmp(argv[1], "receive") == 0) {
        if (argc < 3)
            return 1;
        mgit_receive(argv[2]);
    } else if (strcmp(argv[1], "show") == 0) {
        mgit_show(argc > 2 ? argv[2] : NULL);
    } else if (strcmp(argv[1], "restore") == 0) {
        if (argc < 3)
            return 1;
        mgit_restore(argv[2]);
    }
    return 0;
}

void mgit_init()
{
    // TODO: Safely initialize the repository structure.
    // HINT: Check if ".mgit" already exists using stat(). If it does, do NOTHING
    // to prevent accidental data destruction.

    // TODO: Create the following directories with 0755 permissions:
    // 1. ".mgit"
    // 2. ".mgit/snapshots"

    // TODO: Create the vault file ".mgit/data.bin".
    // HINT: Open with O_CREAT | O_WRONLY and 0644 permissions. Do NOT use O_TRUNC!

    // TODO: Create ".mgit/HEAD" and write "0" into it to initialize the snapshot counter.

    struct stat st;
    if (stat(".mgit", &st) == 0) {
        return;
    } else if (errno != ENOENT) {
        exit(EXIT_FAILURE);
    } else if (mkdir(".mgit", 0755) == -1) {
        exit(EXIT_FAILURE);
    }

    if (mkdir(".mgit/snapshots", 0755) == -1) {
        exit(EXIT_FAILURE);
    }

    int fd = open(".mgit/data.bin", O_CREAT | O_WRONLY, 0644);
    if (fd == -1) {
        exit(EXIT_FAILURE);
    }
    close(fd);

    fd = open(".mgit/HEAD", O_CREAT | O_WRONLY, 0644);
    if (fd == -1) {
        exit(EXIT_FAILURE);
    }

    if (write(fd, "0", 1) == -1) {
        close(fd);
        exit(EXIT_FAILURE);
    }

    close(fd);
}

void mgit_show(const char* id_str)
{
    // --- LIVE VIEW ---
    if (id_str == NULL) {
        printf("=== LIVE VIEW ===\n");

        FileEntry* list = build_file_list_bfs(".", NULL);
        FileEntry* curr = list;

        while (curr) {
            printf("%s", curr->path);

            if (curr->is_directory)
                printf(" [DIR]");
            else
                printf(" (%ld bytes)", curr->size);

            printf("\n");

            curr = curr->next;
        }

        free_file_list(list);
        return;
    }

    // --- SNAPSHOT VIEW ---
    uint32_t id = atoi(id_str);

    Snapshot* snap = load_snapshot_from_disk(id);
    if (!snap) {
        fprintf(stderr, "Error: Snapshot %d not found.\n", id);
        exit(1);
    }

    printf("=== SNAPSHOT %u ===\n", snap->snapshot_id);
    printf("Message: %s\n", snap->message);
    printf("Files:\n");

    FileEntry* curr = snap->files;

    while (curr) {
        printf("%s", curr->path);

        if (curr->is_directory)
            printf(" [DIR]");
        else
            printf(" (%ld bytes)", curr->size);

        printf("\n");

        curr = curr->next;
    }

    free_file_list(snap->files);
    free(snap);
}
