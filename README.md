# goit-de-fp
<!-- mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env -->

## Завдання 1 

**Висновок:**  

Завдання виконано успішно. Розроблено стримінгове рішення, яке забезпечує ефективну обробку даних фізичних показників та результатів виступів атлетів. Всі етапи, передбачені завданням, реалізовано за допомогою Apache Spark:  

1. Дані фізичних показників атлетів були успішно зчитані з MySQL бази даних, очищені від некоректних значень та підготовлені для аналізу.  
2. Результати змагань зчитано з MySQL, опубліковано у Kafka-топік, а потім трансформовано у структурований формат для подальшої обробки.  
3. Дані з результатів виступів та біологічні дані було об'єднано за унікальним ключем `athlete_id`.  
4. Проведено агрегацію для обчислення середнього зросту і ваги атлетів за видами спорту, типом медалі, статтю та країною походження. Додано часову мітку для кожної обчисленої групи.  
5. Результати розрахунків було спрямовано як у вихідний Kafka-топік, так і в MySQL базу даних для зберігання та подальшого використання.  


## Завдання 2

**Висновки:**

### 1. **ETL-Процес**
- Вхідні дані завантажуються у форматі CSV з FTP-сервера.
- Обробка на рівнях:
  - **Bronze**: збереження сирих даних у Parquet для швидшого доступу.
  - **Silver**: очистка тексту, дедублікація.
  - **Gold**: агрегація середніх значень ваги й зросту для аналітики.

### 2. **Технічні аспекти**
- Розділення на рівні обробки (landing, bronze, silver, gold) покращує структуру пайплайну.
- Використання Apache Spark забезпечує ефективність роботи з великими даними.
- Автоматизація через Airflow DAG зменшує ризик помилок і спрощує управління задачами.

### 3. **Результати**
- **Bronze**: дані у сирому вигляді.
- **Silver**: очищені, структуровані, без дублікатів.
- **Gold**: агреговані дані, готові до аналітики.

### 4. **Рекомендації**
- Додати логування і перевірку якості даних.
- Впровадити тестування пайплайнів.

Рішення — масштабоване, автоматизоване, з якісними даними для аналітики чи інтеграції з BI-системами.
