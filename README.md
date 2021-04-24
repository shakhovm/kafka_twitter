# Kafka Twitter


Command to run producer
--------------------------

```
$ python producer.py <path-to-file>
```
\<path-to-fil\> - path to file with data from twitter dataset
  
  
Command to run consumer
--------------------------

```
$ python consumer.py <n> <storage flag> <path to result file> <key json>
```
 \<n\> - paramater for tasks
 
 \<storage flag\> - --google-cloud or --local-file
 
 \<path to resilt file\> - path to file for saving results
 
 \<key json\> - json-file with key for connecting to google cloud storage
 
# Опис компонент

1. Producer - читає дані з заданого файлу, та пушить їх в кафку зі швидкістю 30-50 t/s, (для симуляції twitter api, кожного разу записує з рандомною швидкістю між 30-50 t/s). Для виконання завдання нам потрібні тільки account, message та date - де, date - це час, коли ми зчитали з файлу (для симуляції twitter api)
2. Consumer - отримує дані з кафки та записує їх в датафрейм, коли consumer отримує дані, які producer зчитав пізніше запуску консюмера, то він перестає зчитувати і будує на основі отриманих даних репорти. Файл з результатами записує або в google cloud storage bucket або в локальний файл (як задав користувач).
# Репорти
example.json - приклад репортів у форматі
{
   "task1": ...,
   "task2": ...,
   "task3": ...,
   "task4": ...,
   "task5": ...
}
