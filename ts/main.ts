import { BehaviorSubject, fromEvent, combineLatest, map } from 'rxjs';

// 1. Описуємо інтерфейси та типи (Вимога 4)
interface Task {
    id: number;
    text: string;
    completed: boolean;
}

type FilterType = 'all' | 'active' | 'completed';

// 2. Створюємо реактивні контейнери стану (Вимога 1)
// BehaviorSubject зберігає останнє значення і одразу віддає його новим підписникам
const tasks$ = new BehaviorSubject<Task[]>([]);
const filter$ = new BehaviorSubject<FilterType>('all');

document.addEventListener('DOMContentLoaded', () => {
    // Отримуємо елементи DOM
    const input = document.getElementById('task-input') as HTMLInputElement;
    const addBtn = document.getElementById('add-btn') as HTMLButtonElement;
    const list = document.getElementById('task-list') as HTMLUListElement;
    const filterBtns = document.querySelectorAll('.filter-btn');

    // ==========================================
    // 3. Обробка дій користувача (Вимога 2)
    // ==========================================

    // Перетворюємо клік на кнопку "Додати" у потік подій
    fromEvent(addBtn, 'click')
        .pipe(
            // Витягуємо текст з інпута та обрізаємо пробіли
            map(() => input.value.trim())
        )
        .subscribe(text => {
            if (text) {
                // Створюємо нову задачу
                const newTask: Task = { id: Date.now(), text, completed: false };
                // Беремо поточний масив, додаємо нову задачу і пушимо в потік
                tasks$.next([...tasks$.getValue(), newTask]);
                input.value = ''; // Очищаємо інпут
            }
        });

    // Перетворюємо кліки по кнопках фільтрів у потік
    filterBtns.forEach(btn => {
        fromEvent(btn, 'click')
            .pipe(
                // Витягуємо значення data-filter з натиснутої кнопки
                map(e => (e.target as HTMLButtonElement).dataset.filter as FilterType)
            )
            .subscribe(filterValue => {
                // Оновлюємо стан фільтра
                filter$.next(filterValue);

                // Візуально перемикаємо активну кнопку
                filterBtns.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
            });
    });

    // ==========================================
    // 4. Автоматичне оновлення інтерфейсу (Вимога 3)
    // ==========================================

    // combineLatest слухає ОДНОЧАСНО потік задач і потік фільтрів.
    // Якщо змінюється хоча б щось одне — спрацьовує рендер.
    combineLatest([tasks$, filter$]).subscribe(([tasks, currentFilter]) => {

        // Відфільтровуємо масив залежно від поточного стану фільтра
        const filteredTasks = tasks.filter(task => {
            if (currentFilter === 'active') return !task.completed;
            if (currentFilter === 'completed') return task.completed;
            return true; // для 'all'
        });

        // Очищаємо список перед новим рендером
        list.innerHTML = '';

        // Рендеримо відфільтровані задачі
        filteredTasks.forEach(task => {
            const li = document.createElement('li');
            li.className = task.completed ? 'completed' : '';
            li.textContent = task.text;

            // Додаємо можливість перемикати статус при кліку на саму задачу
            // Це не використовує RxJS напряму для кліку, але оновлює RxJS-стан
            li.addEventListener('click', () => {
                const updatedTasks = tasks$.getValue().map(t =>
                    t.id === task.id ? { ...t, completed: !t.completed } : t
                );
                tasks$.next(updatedTasks);
            });

            list.appendChild(li);
        });
    });
});