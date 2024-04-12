#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <dirent.h>

#define MAX_LINE 100
#define MAX_BUFFER 20
#define FILE_TITLE 100
#define MAX_FILES 1024
#define MAX_SET 1024
#define MAX_STR 1024



struct config {
    char PATH_FILES[MAX_LINE];
    char INVENTORY_FILE[MAX_LINE];
    char LOG_FILE[MAX_LINE];
    int NUM_PROCESOS;
    int NUM_SUCURSALES;
    int SIMULATE_SLEEP;
    int SLEEP_MIN;
    int SLEEP_MAX;
} config;



struct taskSucursal {
    int i;
    char** tasks;
    int max;
    pthread_mutex_t turns; // guarda el titulo de los ficheros que tiene que leer ademas de un indice para saber por cual va ejecutando y semaforos
    int highest;
    sem_t semFiles;
};

struct buffer {
    int indexWrite;
    int indexRead;
    int finished;
    char lines[MAX_BUFFER][MAX_LINE];
} buffer; 

sem_t semRead;
pthread_mutex_t Mutex;
sem_t semWrite;
pthread_mutex_t consMutex;

/* ------------------------------- LOG -------------------------------------*/

FILE* logfp;
pthread_mutex_t logMutex;

void writeLog(char* s){
    pthread_mutex_lock(&logMutex); // escribe en fichero log con puntero global protegiendo la escritura
    printf("%s\n", s);
    fprintf(logfp,"%s\n", s);
    pthread_mutex_unlock(&logMutex);
}

void writeLogDebug(char* s){
    pthread_mutex_lock(&logMutex);// escribe en fichero log con puntero global protegiendo la escritura con mensaje especial debug
    printf("**DEBUG** %s\n", s);
    fprintf(logfp,"**DEBUG**: %s", s);
    pthread_mutex_unlock(&logMutex);
}

void flushLog(){
    pthread_mutex_lock(&logMutex); // S.O descarga buffers internos a disco
    fflush(logfp);
    pthread_mutex_unlock(&logMutex); // el fichero log no se escribe cuando hacemos fprintf, esto fuerza la escritura
}

/* ------------------------------- END OF LOG -------------------------------------*/


/* ------------------------------- SET -------------------------------------*/

struct setFiles {
    char** elems; // estructura que guarda los ficheros leidos para saber si los ha leido o no
    int max;
    int highest;
} processedfiles ;


void set_add(char* s){
    processedfiles.elems[processedfiles.max] = strdup(s); // guarda el titulo cuando lee el fichero
    processedfiles.max++;
    if(processedfiles.max == processedfiles.highest){
    	processedfiles.highest+= MAX_FILES;
        processedfiles.elems = (char**)realloc(processedfiles.elems, sizeof(char*)* processedfiles.highest ); // si ha llegado ha el limete de memoria hace realloc
    }
}

void set_init(){ // inicializa un set con malloc
    processedfiles.highest = MAX_FILES;
    processedfiles.elems = (char**)malloc(sizeof(char*)*MAX_FILES);
}

int set_find(char* s){
    for (int i = 0; i < processedfiles.max; i++){
    	if (!strcmp(s, processedfiles.elems[i])){
    	    return 1;
    	}
    }
    return 0;
}

/* ------------------------------- END OF SET -------------------------------------*/



void readConfig(char* dirConfig);
FILE* consfp;

void readDir(char* nomSucursal, struct taskSucursal* tasks){
    DIR *directory;
    tasks->i = 0; // read dir utiliza un DIR* para recorrer el file path del config y guardar el titulo en la lista de taskSucursal
    char msg[1024];
    tasks->max = 0;
    tasks->highest=MAX_FILES;
    
    printf("reading dir: \"%s\"\n", config.PATH_FILES);
    
    
    struct dirent *entry;
    directory = opendir(config.PATH_FILES); // abre el directorio y detecta error
    if (directory == NULL){
    	printf("file not found\n");
    	return;
    }
    tasks->tasks = (char**)malloc(sizeof(char*) * tasks->highest); //PC array of file titles
    printf("malloc\n");
    while ((entry = readdir(directory)) != NULL) {
        if (strstr(entry->d_name, nomSucursal) != NULL){
	    sprintf(msg, "Sucursal encontrada: \"%s\"", entry->d_name); // recorre el directorio encontrando ficheros que pertenezcan a cierta sucursal
	    writeLog(msg);
	    
	    pthread_mutex_lock(&tasks->turns);
	    
            tasks->tasks[tasks->max] = (char*)malloc(FILE_TITLE); //PC file title string
            strcpy(tasks->tasks[tasks->max], config.PATH_FILES);
            strcat(tasks->tasks[tasks->max], "/");
            strcat(tasks->tasks[tasks->max], entry->d_name); // crea la ruta completa del fichero y la guarda en los tasks
            tasks->max++;
            if (tasks->max == tasks->highest){
            	tasks->highest+= MAX_FILES;
            	tasks->tasks = (char**)realloc(tasks->tasks, (tasks->highest) * sizeof(char*)); //PC array of file titles
            	printf("realloc\n");
            }
            
            pthread_mutex_unlock(&tasks->turns); // protege esta zona porque los tasks son compartidoa
            
            sem_post(&tasks->semFiles);
        }
    }
    if (closedir(directory) != 0) {
        printf("Failed to close directory\n"); // cierra el directorio
    } else {
        printf("closed dir\n");
    }
}


void* ReadFile(void* arg) {
    struct taskSucursal* tasks = (struct taskSucursal*)arg;  // funcion de hilos de lectura de ficheros
    FILE* fp;
    pthread_t tid = pthread_self();
    char file[FILE_TITLE];
    char logmsg[MAX_STR];
    int linesread;
    char* str;
    printf("new thread running\n"); // dice al usuario que es un nuevo hilo
    while (1){
    	sem_wait(&tasks->semFiles);
        pthread_mutex_lock(&tasks->turns); // protegiendo la lista de los tasks porque son datos compartidos
        printf("task recieved: \"%s\" i=%d max=%d highest=%d\n", tasks->tasks[tasks->i], tasks->i, tasks->max, tasks->highest);
        strcpy(file, tasks->tasks[tasks->i]);
        fp = fopen(tasks->tasks[tasks->i], "r"); // lee el turno que corresponde al indice y le suma 1 al indice para pasar al siguiente
        printf("free pointer %d", tasks->i); // en cuanto se lea el titulo del fichero se liberan los datos
        free(tasks->tasks[tasks->i]);
        tasks->i++;
        
        pthread_mutex_unlock(&tasks->turns);
        linesread = 0;
        while (fp != NULL){
            sem_wait(&semRead); // lee el fichero de titulo obtenido en los tasks y va escribiendo linea a linea en el buffer
            pthread_mutex_lock(&Mutex);
            str = buffer.lines[buffer.indexWrite % MAX_BUFFER];
            if (fgets(str, MAX_LINE, fp) == NULL){ // la proteccion del buffer es parecida a el problema producer consumer
                size_t len = strlen(str);
    		if (len > 0 && str[len - 1] == '\n') { // borra el \n del final de cada linea
        		str[len - 1] = '\0';
    		}
            	printf("file finished\n"); // cuando el fgets devuelve null el fichero termino y sale del bucle con un break y resetea semRead porque no ha metido nada en el buffer
            	pthread_mutex_unlock(&Mutex);
            	sem_post(&semRead);
            	break;
            }
            linesread++;
            buffer.indexWrite++;
            pthread_mutex_unlock(&Mutex);
            sem_post(&semWrite); // si mete algo en el buffer hace post a semWrite para que el hilo de escritura pueda escribir
        }
        
        sprintf(logmsg, ":::%lu:::%s:::%d lines read:::", tid, file, linesread); // escribe mensaje en log informando de que ha leido un fichero
        writeLog(logmsg);
        fclose(fp);
    }
    return NULL;
}

void* WriteConsolidado() { // funcion de hilo de escritura en consolidado
    char logmsg[MAX_STR]; 
    while (1){
    	sem_wait(&semWrite);
    	pthread_mutex_lock(&Mutex); 
    	if (buffer.finished && buffer.indexRead==buffer.indexWrite){ // comprueba condicion de salida del while protegida por semaforos
    		pthread_mutex_unlock(&Mutex);
    		break;
    	}
    	        
        pthread_mutex_lock(&consMutex);
        fprintf(consfp, "%s", buffer.lines[buffer.indexRead % MAX_BUFFER]); // en la direccion de consolidado va escribiendo todas las lineas del buffer
        pthread_mutex_unlock(&consMutex);
        
        
        buffer.indexRead++; // cuando lee una linea pasa a la siguiente
        pthread_mutex_unlock(&Mutex);
        sem_post(&semRead);
    }
    return NULL;
}

int main(int argc, char * argv[])
{
    readConfig("fp.conf"); // lee fichero configuracion
    buffer.indexRead = 0;
    buffer.indexWrite = 0; // indices de buffer para control de flujo del programa
    buffer.finished = 0;

      printf("%s=\"%s\"\n","PATH_FILES",config.PATH_FILES); 
      printf("%s=\"%s\"\n","INVENTORY_FILE",config.INVENTORY_FILE); 
      printf("%s=\"%s\"\n","LOG_FILE",config.LOG_FILE); 
      printf("%s=\"%d\"\n","NUM_PROCESOS",config.NUM_PROCESOS); 
      printf("%s=\"%d\"\n","NUM_SUCURSALES",config.NUM_SUCURSALES); 
      printf("%s=\"%d\"\n","SIMULATE_SLEEP",config.SIMULATE_SLEEP);  // demuestra los valores leidos del config por pantalla
      printf("%s=\"%d\"\n","SLEEP_MIN",config.SLEEP_MIN);
      printf("%s=\"%d\"\n","SLEEP_MAX",config.SLEEP_MAX); 
      printf("****************\n");

    
    logfp = fopen(config.LOG_FILE, "w+"); // abre el fichero log que se mantiene abierto hasta el final del programa
    if (logfp == NULL){
    	printf("Could not find log_file");
    	return -1;
    }
    
    char contSuc[6] = "SU000"; // formato nombre de sucursal en el titulo de ficheros
    
    struct taskSucursal* sucArray[config.NUM_SUCURSALES]; // crea una struct con la lista de los ficheros por leer por cada sucursal
    
    
    pthread_t threads[config.NUM_PROCESOS]; // crea los hilos necesarios para lectura y uno para escritura
    pthread_t threadWrite;
    
    sem_init(&semRead, 0, MAX_BUFFER);
    sem_init(&semWrite, 0, 0);
    pthread_mutex_init(&Mutex, NULL); // inicializa todos los semaforos
    pthread_mutex_init(&logMutex, NULL);
    pthread_mutex_init(&consMutex, NULL);
    

    for (int i = 0; i < config.NUM_SUCURSALES; i++){
        sucArray[i] = (struct taskSucursal*)malloc(sizeof(struct taskSucursal));
        pthread_mutex_init(&(sucArray[i]->turns), NULL);
        sprintf(contSuc, "SU%03d" ,(i+1)); // recorre el directorio una vez por sucursal guardando en la lista de ficheros por leer de cada una los titulos de los ficheros de dicha sucursal
        printf("%s\n", contSuc);
        printf("%s\n", config.PATH_FILES);
        sem_init(&sucArray[i]->semFiles, 0, 0);
        readDir(contSuc, sucArray[i]);
    }
    for (int i = 0; i < config.NUM_PROCESOS; i++){
        pthread_create(&threads[i], NULL, ReadFile, (void*)sucArray[i % config.NUM_SUCURSALES]);
    } // lanza la cantidad de hilos definido en el config para que vayan leyendo sus ficheros
    
    consfp = fopen("consolidado.csv", "w");
    pthread_create(&threadWrite, NULL, WriteConsolidado, "consolidado.csv"); // lanza un hilo que va escribiendo en consolidado.csv
    
    // while(1){
    	
    
    	sleep(20);
    	pthread_mutex_lock(&consMutex);
        fflush(consfp); // fuerza al SO a escribir en consolidado
        pthread_mutex_unlock(&consMutex);
        flushLog();
    // }
   

    for (int i = 0; i < config.NUM_PROCESOS; i++){
    	printf("joining read thread %d \n", i); // espera a que mueran los hilos de lectura
        pthread_join(threads[i], NULL);
    }
    for (int i = 0; i < config.NUM_SUCURSALES; i++){ // libera todos los espacios de memoria utilizados para manejar la lectura de ficheros
    	free(sucArray[i]->tasks);
        free(sucArray[i]);
        printf("freed memory for tasks of suc %d\n", i+1);
    }
    pthread_mutex_lock(&Mutex);
    buffer.finished = 1;
    pthread_mutex_unlock(&Mutex); // levanta flag de finished para indicar al hilo de escritura que puede morir
    sem_post(&semWrite);
    pthread_join(threadWrite, NULL); // espera a que muera el hilo de escritura
    
    fclose(consfp);
    fclose(logfp); // cierra los ficheros abiertos
    
    sem_destroy(&semWrite);
    sem_destroy(&semRead);
    pthread_mutex_destroy(&Mutex); // destruye semaforos
    pthread_mutex_destroy(&consMutex);
    pthread_mutex_destroy(&logMutex);
    
    return 0;
}

void readConfig(char* dirConfig){ // read config lee el archivo config de direccion pasado por parametro
     FILE* fp;
     fp = fopen(dirConfig, "r");
     char line[MAX_LINE]; // abre el archivo y lo recorre separando las lineas por un =
     char *key, *value;
     while(fgets(line, MAX_LINE, fp) != NULL){ // utiliza el string antes del = como key para comparar con los parametros que busca
        key = strtok(line, "=");
        value = strtok(NULL, "\n");  // el valor va despues del '=' pero tenemos que separar por '\n' para que no lea la separacion de linea
        if (key && value) {
            if (strcmp(key, "PATH_FILES") == 0) { 
                strcpy(config.PATH_FILES, value);
            } else if (strcmp(key, "INVENTORY_FILE") == 0) {
                strcpy(config.INVENTORY_FILE, value);
            } else if (strcmp(key, "LOG_FILE") == 0) { // compara el key con todas las opciones para ir asignando los values
                strcpy(config.LOG_FILE, value);
            } else if (strcmp(key, "NUM_PROCESOS") == 0) {
                config.NUM_PROCESOS = atoi(value);
            } else if (strcmp(key, "NUM_SUCURSALES") == 0) {
                config.NUM_SUCURSALES = atoi(value);
            } else if (strcmp(key, "SIMULATE_SLEEP") == 0) {
                config.SIMULATE_SLEEP = atoi(value);
            } else if (strcmp(key, "SLEEP_MIN") == 0) {
                config.SLEEP_MIN = atoi(value);
            } else if (strcmp(key, "SLEEP_MAX") == 0) {
                config.SLEEP_MAX = atoi(value);
            }
        }
        printf("read line: %s \n", key);
    }
    fclose(fp);
    printf("closed read\n");
}

/*-------------------------------------------------------------MONITORING------------------------------------------------------------------*/




