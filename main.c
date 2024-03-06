#define _GNU_SOURCE

#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

#define EMAIL_MAX 255
#define NAME_MAX 32
#define MAX_PAGES 31
#define SUCCESS 1
#define ERROR 0

char *db_file = "data.db";

typedef struct{
    char *buffer;
    size_t buffer_length;
    __ssize_t input_length;
}InputBuffer;

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

typedef enum{ INSERT, SELECT, PRINT}StatementType;

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

typedef enum { APPEND, WRITE} WriteType;

typedef struct{
    u_int32_t id;                   //4 bytes
    char username[NAME_MAX + 1];    //33 bytes
    char email[EMAIL_MAX + 1];      //256 bytes
}Row;

const u_int32_t USERNAME_OFFSET = sizeof(u_int32_t);
const u_int32_t EMAIL_OFFSET = USERNAME_OFFSET + 33;
const u_int32_t ROW_OFFSET = 296;

const u_int32_t PAGE_SIZE = 4096;
const u_int32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_OFFSET;
const u_int32_t MAX_ROWS = ROWS_PER_PAGE * MAX_PAGES;

//common node layout
const u_int32_t NODE_TYPE_SIZE = sizeof(u_int8_t);
const u_int32_t NODE_TYPE_OFFSET = 0;
const u_int32_t IS_ROOT_SIZE = sizeof(u_int8_t);
const u_int32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const u_int32_t PARENT_POINTER_SIZE = sizeof(u_int32_t);
const u_int32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const u_int8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

//leaf node layout
const u_int32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(u_int32_t);
const u_int32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const u_int32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
// Leaf node body layout
const u_int32_t LEAF_NODE_KEY_SIZE = sizeof(u_int32_t);
const u_int32_t LEAF_NODE_KEY_OFFSET = 0;
const u_int32_t LEAF_NODE_VALUE_SIZE = ROW_OFFSET;
const u_int32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const u_int32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const u_int32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const u_int32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

//internal node layout
const u_int32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(u_int32_t);
const u_int32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const u_int32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(void*);
const u_int32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const u_int32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
//internal node body layout
const u_int32_t INTERNAL_NODE_KEY_SIZE = sizeof(u_int32_t);
const u_int32_t INTERNAL_NODE_CHILD_SIZE = sizeof(void*);
const u_int32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

typedef struct
{
    size_t number_of_pages;
    int file_descriptor;
    size_t file_length;
    void *pages[MAX_PAGES];
}Pager;

void *add_page(Pager *pager){
    pager->pages[pager->number_of_pages] = malloc(PAGE_SIZE);
    pager->number_of_pages++;
    return pager->pages[pager->number_of_pages - 1];
}

typedef struct 
{
    StatementType type;
    Row row_toinsert;
}Statement;

typedef struct{
    Pager *pager;
    size_t root_page_num;
}Table;

typedef struct{
    Table *table;
    u_int32_t page_num;
    u_int32_t cell_num;
    bool table_end;
}Cursor;

u_int32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}
void* leaf_node_cell(void* node, u_int32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}
u_int32_t* leaf_node_key(void* node, u_int32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}
void* leaf_node_value(void* node, u_int32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

NodeType node_type(void *node){
    return *(u_int8_t*)(node + NODE_TYPE_OFFSET);
}
void set_node_type(void *node, NodeType type){
    u_int8_t value = type;
    memcpy(node + NODE_TYPE_OFFSET, &value, sizeof(u_int8_t));
}

bool is_root(void *node){
    return *(bool*)(node + IS_ROOT_OFFSET);
}
void set_is_root(void *node, bool value){
    *(u_int8_t*)(node + IS_ROOT_OFFSET) = value;
}

void initialize_leaf_node(void* node) { 
    *leaf_node_num_cells(node) = 0; 
    set_node_type(node, NODE_LEAF);
    set_is_root(node, false);
}

u_int32_t *internal_num_keys(void *node){
    if(node_type(node) == NODE_INTERNAL){
        return (node + INTERNAL_NODE_NUM_KEYS_OFFSET);
    }else{
        printf("ERROR: passed leaf node as internal");
        return ERROR;
    }
}
void internal_set_num_keys(void *node, u_int32_t num_keys){
    if(node_type(node) == NODE_INTERNAL){
        *(u_int32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET) = num_keys;
    }
}

u_int32_t* internal_cell_key(void *node, u_int32_t key_pos){
    return (node + INTERNAL_NODE_HEADER_SIZE + (INTERNAL_NODE_CELL_SIZE * key_pos));
}

void **internal_child(void *node, u_int32_t child_pos){
    return (node + INTERNAL_NODE_RIGHT_CHILD_OFFSET + (INTERNAL_NODE_CELL_SIZE * child_pos));
}
void internal_add_child(void *node, u_int32_t key,void *child){

    u_int32_t *num_keys = internal_num_keys(node);
    if(num_keys){
        *(u_int32_t*)(node + INTERNAL_NODE_HEADER_SIZE + (INTERNAL_NODE_CELL_SIZE * (*num_keys))) = key;
        *(void**)(node + INTERNAL_NODE_HEADER_SIZE + (INTERNAL_NODE_CELL_SIZE * (*num_keys)) + INTERNAL_NODE_KEY_SIZE) = child;
        internal_set_num_keys(node, *num_keys + 1);
    }else{
        printf("pointer is bad.\n");
    }
}

void initialize_internal_node(void *node){
    internal_set_num_keys(node, 0);
    set_node_type(node, NODE_INTERNAL);
    set_is_root(node, false);
}

void* create_new_root(Table* table, void *right_child){
    void *left_child = add_page(table->pager);
    void *root = table->pager->pages[table->root_page_num];
    memcpy(left_child, root, PAGE_SIZE);
    *(u_int8_t*)(left_child + IS_ROOT_OFFSET) = false;
    set_node_type(left_child, NODE_LEAF);

    set_node_type(root, NODE_INTERNAL);    
    internal_set_num_keys(root, 0);
    memcpy((root + INTERNAL_NODE_RIGHT_CHILD_OFFSET), &left_child, sizeof(void*));
    u_int32_t *left_child_biggest_key = leaf_node_key(left_child, *leaf_node_num_cells(left_child) - 1);
    if(left_child_biggest_key == ERROR){
        printf("Error:couldn't get key\n");
    }
    internal_add_child(root, *left_child_biggest_key, right_child);

    return root;
}


Table *new_table(){
    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = (Pager*)malloc(sizeof(Pager));
    for(int i = 0; i < MAX_PAGES; i++){
        table->pager->pages[i] = NULL;
    }
    table->pager->number_of_pages = 0;
    table->root_page_num = 0;
    table->pager->pages[0] = add_page(table->pager);
    initialize_leaf_node(table->pager->pages[0]);
    set_is_root(table->pager->pages[0], true);
    return table;
}

void add_to_db(Pager *pager, u_int32_t page_num){

    if(pager->number_of_pages <= page_num){
        printf("Not a valid page number was passed.\n");
        return;
    }
    if(pager->pages[page_num] == NULL){
        printf("Not a valid page was passed.\n");
        return;
    }
    if(pager->file_descriptor == -1){
        printf("Not able to open file.\n");
        return;
    }
    lseek(pager->file_descriptor, PAGE_SIZE * page_num, SEEK_SET);
    size_t written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    //printf("wrote %i bytes.\n", written);
}

Table *open_table(const char *filename){
    Table *table = new_table();    
    table->pager->file_descriptor = open(filename, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    if(table->pager->file_descriptor == -1){
        printf("not able to get file to get table.\n");
    }
    
    table->pager->file_length = lseek(table->pager->file_descriptor, 0, SEEK_END);
    lseek(table->pager->file_descriptor, 0, SEEK_SET);

    for(int i = 0; i < (int)ceil(table->pager->file_length / PAGE_SIZE) && i < MAX_PAGES; i++){
        void *node;
        if(i){node = add_page(table->pager);}
        else{node = table->pager->pages[0];}
        read(table->pager->file_descriptor, node, PAGE_SIZE);

        int cell_num = 0;
        while(cell_num < ROWS_PER_PAGE){
            if(*(int*)(leaf_node_value(node, cell_num)) == 0){
                break;
            }
            cell_num++;
        }
        *leaf_node_num_cells(table->pager->pages[i]) = cell_num;
    }
    if(table == NULL){
        return ERROR;
    }
    return table;
}

void close_table(Table *table){
    for(int i = 0; i < table->pager->number_of_pages; i++){
        free(table->pager->pages[i]);
    }
    close(table->pager->file_descriptor);
    free(table->pager);
    free(table);
}

Cursor *cursor_start(Table *table){
    Cursor *cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->table_end = false;

    cursor->cell_num = 0;
    cursor->page_num = 0;

    return cursor;
}
Cursor *cursor_end(Table *table){
    Cursor *cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->table_end = true;

    return cursor;
}

void close_cursor(Cursor *cursor){
    free(cursor);
}

void cursor_advance(Cursor *cursor){
    if(cursor->table_end){
        printf("cant increment.\n");
        return;
    }
    if(cursor->cell_num >= LEAF_NODE_MAX_CELLS){
        cursor->cell_num = 0;
        cursor->page_num++;
        return;
    }
    cursor->cell_num++;
}

void read_input(InputBuffer* input_buffer) {
    __ssize_t bytes_read =
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

int prepare_insert(InputBuffer *input_buffer, Statement *statement){
    statement->type = INSERT;

    char *keyword = strtok(input_buffer->buffer, " ");
    char *string_id = strtok(NULL, " ");
    char *string_username = strtok(NULL, " ");
    char *string_email = strtok(NULL, " ");

    if(string_id == NULL || string_email == NULL || string_username == NULL){
        printf("Error: not enough arguments.\n");
        return ERROR;
    }

    int id = atoi(string_id);
    if(id <= 0){
        printf("ID can't be negative or zero.\n");
        return ERROR;
    }
    if(strlen(string_username) > NAME_MAX){
        printf("Error: username too long.\n");
        return ERROR;
    }
    if(strlen(string_email) > EMAIL_MAX){
        printf("Error: email too long.\n");
        return ERROR;
    }
    
    statement->row_toinsert.id = id;
    strcpy(statement->row_toinsert.username, string_username);
    strcpy(statement->row_toinsert.email, string_email);
    
    return SUCCESS;
}

int prepare_statement(InputBuffer *input_buffer, Statement *statement){
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        return prepare_insert(input_buffer, statement);
    }else if(strncmp(input_buffer->buffer, "select", 6) == 0){
        statement->type = SELECT;
        return SUCCESS;
    }else if(strncmp(input_buffer->buffer, "print", 9) == 0){
        statement->type = PRINT;
        return SUCCESS;
        
    }
    else{
        printf("unrecognized command '%s'\n", input_buffer->buffer);
        return ERROR;
    }
}
/*returns cell number*/
int leaf_node_find_cell(void *node, u_int32_t key){
    u_int32_t num_cells = *leaf_node_num_cells(node);

    int max = num_cells;
    int min = 0;
    u_int32_t mid;
    while (max != min)
    {   
        mid = (max + min) / 2;
        //printf("mid = %i\n", mid);
        if(key > *leaf_node_key(node, mid)){
            min = mid + 1;
            continue;
        }
        if(key < *leaf_node_key(node, mid)){
            max = mid;
            continue;
        }
        if(key == *leaf_node_key(node, mid)){
            printf("Error: Duplicate key.\n");
            return -1;
        }
    }
    
    return max;
}

int leaf_node_insert(Cursor *cursor, u_int32_t key, Row *value);
void *split_leaf_node(Cursor *cursor, void *node1){
    void *node2 = add_page(cursor->table->pager);
    initialize_leaf_node(node2);

    u_int32_t *node1_num_cells = leaf_node_num_cells(node1);
    u_int32_t node2_copied_cells = floor(*node1_num_cells/2);

    cursor->page_num = cursor->table->pager->number_of_pages - 1;
    printf("page_num = %i\n", cursor->page_num);
    while(*node1_num_cells - 1 > node2_copied_cells){
        u_int32_t cell_to_copy = *node1_num_cells - 1;
        leaf_node_insert(cursor, *leaf_node_key(node1, cell_to_copy), leaf_node_value(node1, cell_to_copy));
        memcpy(leaf_node_cell(node1, cell_to_copy), leaf_node_cell(node2, 12), LEAF_NODE_CELL_SIZE);
        *leaf_node_num_cells(node1) = cell_to_copy;
    }

    if(is_root(node1)){
        return create_new_root(cursor->table, node2);
    }else if(!is_root(node1)){
        printf("didn't implement this yet\n");
        return ERROR;
    }

}

int leaf_node_insert(Cursor *cursor, u_int32_t key, Row *value){
    void *node = cursor->table->pager->pages[cursor->page_num];
    u_int32_t num_cells = *leaf_node_num_cells(node);

    
    int cell =  leaf_node_find_cell(node, key);
    if (cell == -1){
        return ERROR;
    }
    cursor->cell_num = cell;    
    
    if(num_cells >= LEAF_NODE_MAX_CELLS){
        void* root = split_leaf_node(cursor, node);
        u_int32_t *root_num_keys = internal_num_keys(root);
        if(root_num_keys == NULL){
            printf("Error: Root doesn't have keys.\n");
        }
        u_int32_t root_key = *internal_cell_key(root, *root_num_keys - 1);
        if(root_key < key){
            node = *internal_child(root, *root_num_keys);
        }else{
            node = *internal_child(root, *root_num_keys - 1);
        }

        if(node == NULL){
            printf("node is null\n");
        }

        num_cells = *leaf_node_num_cells(node);
        printf("num_cells: %i\n", num_cells);
        
        int cell =  leaf_node_find_cell(node, key);
        cursor->cell_num = cell;    
        printf("cell: %i\n");
    }
    
    if(cursor->cell_num < num_cells){
        for(int i = num_cells; i > cursor->cell_num; i--){
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    memcpy(leaf_node_value(node, cursor->cell_num), value, ROW_OFFSET);
    *leaf_node_key(node, cursor->cell_num) = key;
    *leaf_node_num_cells(node) += 1;

    return SUCCESS;
}

void leaf_node_print(void *node){
    u_int32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %i).\n", num_cells);
    for(int i = 0; i < num_cells; i++){
        printf("\t%i : %i\n", i, *leaf_node_key(node, i));
    }
}

int do_meta_command(InputBuffer *input_buffer, Table *table){
    if(strcmp(input_buffer->buffer, ".exit") == 0){
        exit(EXIT_SUCCESS);
    }else if(strcmp(input_buffer->buffer, ".btree") == 0){
        leaf_node_print(table->pager->pages[0]);
        return SUCCESS;
    }
    else{
        printf("unrecognized command '%s'\n", input_buffer->buffer);
        return ERROR;
    }
}

int insert(Statement *statement, Cursor *cursor){
    Row *row = (Row*)malloc(sizeof(Row));
    row->id = statement->row_toinsert.id;
    strcpy(row->email, statement->row_toinsert.email);
    strcpy(row->username, statement->row_toinsert.username);

    if(cursor->table->pager->pages[cursor->page_num] == NULL){
        add_page(cursor->table->pager);
    }
    
    if(leaf_node_insert(cursor, row->id, row)){
        printf("Inserted.\n");
        add_to_db(cursor->table->pager, cursor->page_num);
        cursor_advance(cursor);
    }

    return SUCCESS;
}

int print(Statement *statement, Table *table){
    for(int i = 0; i < table->pager->number_of_pages; i++){
        void *node = table->pager->pages[i];
        if(node_type(node) == NODE_LEAF){
            for(int j = 0; j < LEAF_NODE_MAX_CELLS && j < *(int*)leaf_node_num_cells(node); j++){
                void *value = leaf_node_value(node, j);
                    
                printf("(%u, %s, %s)\n", *(unsigned int*)value, 
                                        (char*)(value + USERNAME_OFFSET), 
                                        (char*)(value + EMAIL_OFFSET));
            }
        }
    }
}

int execute_statement(Statement *statement, Cursor *cursor){
    switch (statement->type)
    {
    case INSERT:
        insert(statement, cursor);
        break;
    case SELECT:
        
        break;
    case PRINT:
        print(statement, cursor->table);
        break;
    default:
        break;
    }
}

int main(int argc, char *argv[]){
    char *filename;
    if(argc == 1){
        filename = db_file;
    }else{
        filename = argv[1];
    }

    Table *table = open_table(filename);
    Cursor *cursor = cursor_start(table);

    InputBuffer *input_buffer = new_input_buffer();
    while(true){
        printf("(sqlite)> ");
        read_input(input_buffer);

        if(input_buffer->buffer[0] == '.'){
            do_meta_command(input_buffer, table);
            continue;
        }
        
        Statement statement;
        if (prepare_statement(input_buffer, &statement)){
            execute_statement(&statement, cursor);
        }
    }
    close_cursor(cursor);
    close_table(table);
    close_input_buffer(input_buffer);
}
