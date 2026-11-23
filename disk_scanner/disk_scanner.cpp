#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <filesystem>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>

using namespace std;
namespace fs = std::filesystem;

pthread_mutex_t static_mutex = PTHREAD_MUTEX_INITIALIZER;
int count_files = 0, max_depth = -1, n_threads = 2, n_top = 5;
uint64_t weight_files = 0;
unordered_map<string, int> dict_of_dir, dict_of_files;

struct Task
{
    char *path;
    int depth;
    Task *next;
};

struct Queue
{
    Task *head;
    Task *tail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int outstanding;
    int shutting_down;
};

void queue_init(Queue *q)
{
    q->head = q->tail = NULL;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
    q->outstanding = 0; q->shutting_down = 0;
}

void queue_destroy(Queue *q)
{
    Task *t = q->head;
    while (t)
    {
        Task *n = t->next;
        free(t->path);
        delete t;
        t = n;
    }

    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
}

void queue_push(Queue *q, const char *path, int depth)
{
    Task *t = new Task;
    t->path = strdup(path);
    t->depth = depth;
    t->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail) q->tail->next = t;
    else q->head = t;

    q->tail = t;
    q->outstanding++;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

char *queue_pop(Queue *q, int *out_depth)
{
    pthread_mutex_lock(&q->mutex);

    while (q->head == NULL && q->outstanding > 0 && !q->shutting_down)
    {
        pthread_cond_wait(&q->cond, &q->mutex);
    }

    if (q->shutting_down || q->head == NULL)
    {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }

    Task *t = q->head;
    q->head = t->next;
    if (!q->head) q->tail = NULL;

    char *path = t->path;
    *out_depth = t->depth;
    delete t;

    pthread_mutex_unlock(&q->mutex);
    return path;
}

void queue_done(Queue *q)
{
    pthread_mutex_lock(&q->mutex);
    q->outstanding--;
    if (q->outstanding == 0)
    {
        pthread_cond_broadcast(&q->cond);
    }
    pthread_mutex_unlock(&q->mutex);
}

void *worker(void *arg)
{
    Queue *q = (Queue *)arg;

    while (true)
    {
        uint64_t local_count = 0, local_weight = 0;
        int current_depth = 0;
        unordered_map<string, uint64_t> local_files;
        char *dir = queue_pop(q, &current_depth);
        if (!dir) break;

        try
        {
            auto options = fs::directory_options::skip_permission_denied;
            for (auto it = fs::directory_iterator(dir, options); 
                it != fs::directory_iterator(); it++)
            {
                // string filename = it->path().filename().string();
                if (fs::is_regular_file(it->symlink_status()))
                {
                    local_count++;
                    local_weight += fs::file_size(it->path());

                    local_files[it->path().string()] = fs::file_size(it->path());
                }
                else if (fs::is_directory(it->symlink_status()))
                {
                    if (max_depth == -1 || current_depth < max_depth) queue_push(q, it->path().c_str(), current_depth + 1);
                }
            }
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }

        pthread_mutex_lock(&static_mutex);
        count_files += local_count;
        weight_files += local_weight;
        dict_of_dir[dir] = local_count;
        for (auto &[p, size] : local_files) dict_of_files[p] = size;
        pthread_mutex_unlock(&static_mutex);

        free(dir);
        queue_done(q);
    }

    return NULL;
}

void print_info()
{
    // преобразование в вектор пар и сортировка по возрастанию
    vector<pair<string, int>> dict_of_files_ordered(dict_of_files.begin(), dict_of_files.end());
    sort(dict_of_files_ordered.begin(), dict_of_files_ordered.end(),
    [](const auto& a, const auto& b) 
    {
        return a.second > b.second;
    });
    int i = 0;

    cout << "Общее кол-во файлов: " << count_files << " Их суммарный размер: " << weight_files << endl;
    cout << "Кол-во файлов в каждой просмотренной папке" << endl;
    for (const auto &[key, value] : dict_of_dir)
    {
        cout << key << ": " << value << endl;
    }
    cout << "Вывод топ-N самых больших файлов" << endl;
    for (const auto &[key, value] : dict_of_files_ordered)
    {
        if (i == n_top) break;
        cout << key << ": " << value << endl;
        i++;
    }
}
 
int main(int argc, char *argv[])
{
    char *start_dir = nullptr;
    
    for (int i = 1; i + 1 < argc; i += 2) 
    {
        if (strcmp(argv[i], "--dir") == 0) start_dir = argv[i + 1];
        else if (strcmp(argv[i], "--threads") == 0) n_threads = atoi(argv[i + 1]);
        else if (strcmp(argv[i], "--depth") == 0) max_depth = atoi(argv[i + 1]);
        else if (strcmp(argv[i], "--top") == 0) n_top = atoi(argv[i + 1]);
    }

    if (!start_dir)
    {
        cout << "введите директорию" << endl;
        return 1;
    }

    auto start_time = std::chrono::steady_clock::now();

    Queue q;
    queue_init(&q);
    queue_push(&q, start_dir, 0);

    pthread_t *threads = new pthread_t[n_threads];

    for (int i = 0; i < n_threads; i++) pthread_create(&threads[i], NULL, worker, &q);
    for (int i = 0; i < n_threads; i++) pthread_join(threads[i], NULL);

    auto end_time = std::chrono::steady_clock::now();

    print_info();

    std::chrono::duration<double> elapsed = end_time - start_time;
    cout << "Затраченное время: " << elapsed.count() << endl;

    delete[] threads;
    queue_destroy(&q);
    return 0;
}