#include "mgit.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>   // For errno 

//Project made by Adil Arya and Eshwar Rajasekar

// Helper to calculate SHA256 using system utility
void compute_hash(const char* path, uint8_t* output)
{
    // TODO: Set up a pipe to capture the output of the sha256sum command.
    // HINT: Use pipe().

    // TODO: Fork a child process.
    // HINT: In the child process:
    //       1. Close the read end of the pipe.
    //       2. Use dup2() to redirect STDOUT to the write end of the pipe.
    //       3. Silence STDERR by opening "/dev/null" and redirecting it with dup2().
    //       4. Use execlp() to run "sha256sum".

    // HINT: In the parent process:
    //       1. Close the write end of the pipe.
    //       2. Read exactly 64 characters (the hex string) from the read end.
    //       3. Convert the hex string into 32 bytes and store it in 'output'.
    //       4. Remember to wait() for the child to finish!

    int pipefd[2];
    
    if (pipe(pipefd) == -1) {
        perror("pipe failed");
        exit(1);
    }

    pid_t pid = fork();
    if (pid == 0) {
        // Child process
        if (close(pipefd[0]) == -1) { // Close read end
            perror("Failed to close read end of pipe");
            exit(1);
        }
        
        if (dup2(pipefd[1], STDOUT_FILENO) == -1) {
            perror("dup2 failed");
            exit(1);
        }

        if (close(pipefd[1]) == -1) { // Close original write end
            perror("Failed to close write end of pipe");
            exit(1);
        }

        int dev_null = open("/dev/null", O_WRONLY);

        if (dev_null == -1) {
            perror("Failed to open /dev/null");
            exit(1);
        }

        if (dup2(dev_null, STDERR_FILENO) == -1) {
            perror("dup2 failed");
            exit(1);
        }

        if (close(dev_null) == -1) {
            perror("Failed to close /dev/null");
            exit(1);
        }
        
        if (execlp("sha256sum", "sha256sum", path, (char*)NULL) == -1) {
            exit(1);
        }
    } else if (pid > 0) {
        // Parent process

        if (close(pipefd[1]) == -1) { // Close write end
            perror("Failed to close write end of pipe");
            exit(1);
        }

        char hash_str[65]; // 64 chars + null terminator; not reading all at once can lead to issues, scanning in a loop is safer
        ssize_t bytes_read = 0;
        size_t total_read = 0;
        while (total_read < 64) {
            bytes_read = read(pipefd[0], hash_str + total_read, 64 - total_read);
            if (bytes_read == -1) {
                perror("Failed to read from pipe");
                exit(1);
            } else if (bytes_read == 0) {
                break; // EOF
            }
            total_read += bytes_read;
        }

        if (total_read != 64) {
            // fprintf(stderr, "Error: Expected to read 64 characters for hash, but got %zu.\n", total_read);
            exit(1);
        }

        if (close(pipefd[0]) == -1) { // Close read end
            perror("Failed to close read end of pipe");
            exit(1);
        }

        int status;
        if (waitpid(pid, &status, 0) == -1) {
            perror("waitpid failed");
            exit(1);
        }

        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            // fprintf(stderr, "Error: sha256sum command failed.\n");
            exit(1);
        }

        hash_str[64] = '\0'; // Null-terminate the string
        // Convert hex string to bytes
        for (int i = 0; i < 32; i++) {
            if (sscanf(&hash_str[i * 2], "%2hhx", &output[i]) != 1) {
                // fprintf(stderr, "Error: Failed to parse hash output.\n");
                exit(1);
            }
        }
    } else {
        // Fork failed
        perror("fork failed");
        exit(1);
    }

}

// Check if file matches previous snapshot (Quick Check)
FileEntry* find_in_prev(FileEntry* prev, const char* path)
{
    // TODO: Iterate through the 'prev' linked list.
    // Return the FileEntry if its path matches the requested path, otherwise return NULL.
    if (prev == NULL || path == NULL) {
        return NULL;
    }

    FileEntry* curr = prev;
    while(curr != NULL){
        if (strcmp(curr->path, path) == 0){
            return curr;
        }        
        curr = curr->next;
    }
    return NULL;
}

// HELPER: Check if an inode already exists in the current snapshot's list
FileEntry* find_in_current_by_inode(FileEntry* head, ino_t inode)
{
    while (head) {
        if (!head->is_directory && head->inode == inode)
            return head;
        head = head->next;
    }
    return NULL;
}

FileEntry* build_file_list_bfs(const char* root, FileEntry* prev_snap_files)
{
    FileEntry *head = NULL, *tail = NULL;

    // TODO: 1. Initialize the Root directory "." and add it to your BFS queue/list.
    FileEntry* root_entry = malloc(sizeof(FileEntry));

    if (!root_entry) {
        return NULL;
    }
    strncpy(root_entry->path, root, sizeof(root_entry->path) - 1);
    root_entry->path[sizeof(root_entry->path) - 1] = '\0';
    struct stat file_stat;
    if (stat(root, &file_stat) == -1) {
        free(root_entry);
        return NULL;
    }
    root_entry->size = file_stat.st_size;
    root_entry->mtime = file_stat.st_mtime;
    root_entry->inode = file_stat.st_ino;
    if (S_ISDIR(file_stat.st_mode)) {
        root_entry->is_directory = 1;
    } else {
        root_entry->is_directory = 0;
    }
    memset(root_entry->checksum, 0, 32);
    root_entry->num_blocks = 0;
    root_entry->chunks = NULL;
    root_entry->next = NULL;

    head = root_entry;
    tail = root_entry;
    FileEntry* curr = head;

    // TODO: 2. Implement Level-Order Traversal (BFS)
    // - Open directories using opendir() and readdir().
    // - Ignore "." and ".." and the ".mgit" folder.
    // - Construct the full file path safely to avoid buffer overflows.
    // - Use stat() to gather size, mtime, inode, and directory status.

    while(curr != NULL){
        if (curr->is_directory == 1){
            DIR* dir = opendir(curr->path);
            if (dir == NULL) {
            } else {
                struct dirent* entry;
                while ((entry = readdir(dir)) != NULL) {
                   if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".mgit") == 0) {
                        continue;
                    }
                    char child_path[4096];
                    strncpy(child_path, curr->path, sizeof(child_path) - 1);
                    child_path[sizeof(child_path) - 1] = '\0';
                    strncat(child_path, "/", sizeof(child_path) - strlen(child_path) - 1);
                    strncat(child_path, entry->d_name, sizeof(child_path) - strlen(child_path) - 1);
                    FileEntry* child_entry = malloc(sizeof(FileEntry));

                    if (child_entry == NULL) {
                        continue;
                    }
                    strncpy(child_entry->path, child_path, sizeof(child_entry->path) - 1);
                    child_entry->path[sizeof(child_entry->path) - 1] = '\0';

                    struct stat child_stat;
                    if (stat(child_path, &child_stat) == -1) {
                        free(child_entry);
                        continue;
                    }
                    child_entry->size = child_stat.st_size;
                    child_entry->mtime = child_stat.st_mtime;
                    child_entry->inode = child_stat.st_ino;
                    if (S_ISDIR(child_stat.st_mode)) {
                        child_entry->is_directory = 1;
                        child_entry->num_blocks = 0;
                        child_entry->chunks = NULL;
                        memset(child_entry->checksum, 0, 32);
                    } else {
                        child_entry->is_directory = 0;
                            // TODO: 3. Deduplication (Quick Check)
                            // - First, check if the inode was already seen in the CURRENT snapshot (Hard Link).
                            // - Next, check if the file matches the PREVIOUS snapshot (mtime & size match).
                            // - If it matches, copy the checksum and block metadata. DO NOT re-hash.

                        child_entry->num_blocks = 1;
                        child_entry->chunks = malloc(sizeof(BlockTable));
                        if (child_entry->chunks == NULL) {
                            free(child_entry);
                            continue;
                        }
                        child_entry->chunks[0].physical_offset = 0;
                        child_entry->chunks[0].compressed_size = 0;
                        memset(child_entry->checksum, 0, 32);
                        // Check for hard link deduplication
                        FileEntry* existing = find_in_current_by_inode(head, child_entry->inode);
                        if (existing) {
                            child_entry->chunks[0].physical_offset = existing->chunks[0].physical_offset;
                            child_entry->chunks[0].compressed_size = existing->chunks[0].compressed_size;
                            memcpy(child_entry->checksum, existing->checksum, 32);
                        } else {
                            // Check for quick check deduplication against previous snapshot

                            FileEntry* prev_match = find_in_prev(prev_snap_files, child_entry->path);
                            if (prev_match && !prev_match->is_directory && 
                                        prev_match->size == child_entry->size && 
                                        prev_match->mtime == child_entry->mtime) {
                                        
                                        child_entry->chunks[0].physical_offset = prev_match->chunks[0].physical_offset;
                                        child_entry->chunks[0].compressed_size = prev_match->chunks[0].compressed_size;
                                        memcpy(child_entry->checksum, prev_match->checksum, 32);
                                    } else {
                                    // TODO: 4. Deep Check
                                    // - If the file is modified or new, use compute_hash() to generate the SHA-256.
                                    // - Allocate the BlockTable (chunks). Note: physical_offset is set later in storage.c.
                                        compute_hash(child_entry->path, child_entry->checksum);
                                    }                        
                        }

                    }
                    // TODO: 5. Append new FileEntry to your linked list.

                    child_entry->next = NULL;
                    tail->next = child_entry;
                    tail = child_entry;
                    
                

                }
                closedir(dir);
                

            }
            
        } 
        curr = curr->next;

    }




    return head;
}


void free_file_list(FileEntry* head)
{
    // TODO: Iterate through the linked list and free() each node,
    // including the dynamically allocated 'chunks' array within each node.
    FileEntry* curr = head;
    while (curr) {
        FileEntry* next = curr->next;
        if (curr->chunks != NULL) {
            free(curr->chunks);
        }
        free(curr);
        curr = next;
    }
}
