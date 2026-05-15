# Biblioteka — wersja obiektowa
# Programowanie obiektowe: klasy, dziedziczenie, hermetyzacja i polimorfizm


class Book:
    def __init__(self, title, author, total_copies, available_copies=None):
        if total_copies < 0:
            raise ValueError("Liczba sztuk nie może być ujemna.")

        if available_copies is None:
            available_copies = total_copies

        if available_copies < 0 or available_copies > total_copies:
            raise ValueError("Liczba dostępnych sztuk jest niepoprawna.")

        self.title = title
        self.author = author
        self._total_copies = total_copies
        self._available_copies = available_copies

    @property
    def total_copies(self):
        return self._total_copies

    @property
    def available_copies(self):
        return self._available_copies

    @property
    def is_available(self):
        return self._available_copies > 0

    def borrow(self):
        if not self.is_available:
            raise ValueError("Brak dostępnych sztuk tej książki.")

        self._available_copies -= 1

    def return_copy(self):
        if self._available_copies >= self._total_copies:
            raise ValueError("Wszystkie sztuki tej książki są już w bibliotece.")

        self._available_copies += 1

    def __str__(self):
        return (
            f"{self.title} — {self.author} "
            f"(dostępne: {self._available_copies}/{self._total_copies})"
        )


class User:
    def __init__(self, login, password, role):
        self.login = login
        self._password = password
        self.role = role

    def authenticate(self, password):
        return self._password == password

    def menu_options(self):
        raise NotImplementedError("Klasa pochodna musi zdefiniować menu.")


class Reader(User):
    def __init__(self, login, password):
        super().__init__(login, password, "czytelnik")
        self._borrowed_books = []
        self._extension_requests = []

    @property
    def borrowed_books(self):
        return list(self._borrowed_books)

    @property
    def extension_requests(self):
        return list(self._extension_requests)

    def add_borrowed_book(self, book):
        self._borrowed_books.append(book)

    def remove_borrowed_book(self, book):
        self._borrowed_books.remove(book)

    def has_borrowed(self, book):
        return book in self._borrowed_books

    def add_extension_request(self, request):
        self._extension_requests.append(request)

    def menu_options(self):
        return [
            "Przeglądaj katalog",
            "Wypożycz książkę",
            "Moje wypożyczenia",
            "Poproś o przedłużenie",
            "Wyloguj",
        ]


class Librarian(User):
    def __init__(self, login, password):
        super().__init__(login, password, "bibliotekarz")

    def menu_options(self):
        return [
            "Przeglądaj katalog",
            "Lista wszystkich wypożyczeń",
            "Obsługa próśb o przedłużenie",
            "Wyloguj",
        ]


class ExtensionRequest:
    def __init__(self, reader, book):
        self.reader = reader
        self.book = book
        self.status = "oczekująca"

    def accept(self):
        self.status = "zaakceptowana"

    def reject(self):
        self.status = "odrzucona"

    def __str__(self):
        return f"{self.reader.login} — {self.book.title} ({self.status})"


class Library:
    def __init__(self, books=None, users=None):
        self._books = list(books) if books is not None else []
        self._users = list(users) if users is not None else []
        self._extension_requests = []

    @property
    def books(self):
        return list(self._books)

    @property
    def users(self):
        return list(self._users)

    def add_book(self, book):
        self._books.append(book)

    def add_user(self, user):
        self._users.append(user)

    def authenticate(self, login, password):
        for user in self._users:
            if user.login == login and user.authenticate(password):
                return user

        return None

    def find_book_by_title(self, title):
        searched_title = title.lower()

        for book in self._books:
            if book.title.lower() == searched_title:
                return book

        return None

    def borrow_book(self, reader, title):
        if not isinstance(reader, Reader):
            raise ValueError("Tylko czytelnik może wypożyczać książki.")

        book = self.find_book_by_title(title)

        if book is None:
            raise ValueError("Nie znaleziono książki o podanym tytule.")

        book.borrow()
        reader.add_borrowed_book(book)
        return book

    def return_book(self, reader, title):
        if not isinstance(reader, Reader):
            raise ValueError("Tylko czytelnik może oddawać książki.")

        book = self.find_book_by_title(title)

        if book is None or not reader.has_borrowed(book):
            raise ValueError("Czytelnik nie ma wypożyczonej takiej książki.")

        reader.remove_borrowed_book(book)
        book.return_copy()
        return book

    def list_current_loans(self):
        loans = []

        for user in self._users:
            if isinstance(user, Reader):
                for book in user.borrowed_books:
                    loans.append((user.login, book))

        return loans

    def create_extension_request(self, reader, title):
        if not isinstance(reader, Reader):
            raise ValueError("Tylko czytelnik może prosić o przedłużenie.")

        book = self.find_book_by_title(title)

        if book is None or not reader.has_borrowed(book):
            raise ValueError("Możesz przedłużyć tylko książkę, którą masz wypożyczoną.")

        request = ExtensionRequest(reader, book)
        reader.add_extension_request(request)
        self._extension_requests.append(request)
        return request

    def pending_extension_requests(self):
        return [
            request
            for request in self._extension_requests
            if request.status == "oczekująca"
        ]

    def resolve_extension_request(self, index, accepted):
        pending_requests = self.pending_extension_requests()

        if index < 0 or index >= len(pending_requests):
            raise ValueError("Niepoprawny numer prośby.")

        request = pending_requests[index]

        if accepted:
            request.accept()
        else:
            request.reject()

        return request


def create_initial_library():
    books = [
        Book("Lalka", "Bolesław Prus", 3),
        Book("Pan Tadeusz", "Adam Mickiewicz", 2),
        Book("Quo Vadis", "Henryk Sienkiewicz", 4),
        Book("Ferdydurke", "Witold Gombrowicz", 1),
        Book("Zbrodnia i kara", "Fiodor Dostojewski", 2),
    ]

    users = [
        Reader("jan", "1234"),
        Reader("anna", "abcd"),
        Reader("piotr", "pass"),
        Librarian("admin", "admin"),
    ]

    return Library(books, users)


def login_user(library):
    attempts = 0

    while attempts < 3:
        login = input("Login: ")
        password = input("Hasło: ")
        user = library.authenticate(login, password)

        if user is not None:
            print(f"\nZalogowano jako: {user.login} ({user.role})\n")
            return user

        attempts += 1
        print(f"Niepoprawny login lub hasło. Pozostało prób: {3 - attempts}")

    print("Przekroczono limit prób logowania. Program zakończony.")
    return None


def show_catalog(library):
    print("\nKATALOG KSIĄŻEK")
    print("-" * 50)

    for book in library.books:
        print(book)

    print("-" * 50)


def show_menu(user):
    print("\nMENU")

    for index, option in enumerate(user.menu_options(), start=1):
        print(f"{index}. {option}")


def borrow_book_flow(library, reader):
    title = input("Podaj tytuł książki do wypożyczenia: ")

    try:
        book = library.borrow_book(reader, title)
    except ValueError as error:
        print(error)
    else:
        print(f"Wypożyczono książkę: {book.title}")


def show_my_borrowed_books(reader):
    print("\nMOJE WYPOŻYCZENIA")
    print("-" * 50)

    if len(reader.borrowed_books) == 0:
        print("Nie masz aktualnie wypożyczonych książek.")
        return

    for index, book in enumerate(reader.borrowed_books, start=1):
        print(f"{index}. {book.title} — {book.author}")


def create_extension_request_flow(library, reader):
    if len(reader.borrowed_books) == 0:
        print("Nie masz książek, które można przedłużyć.")
        return

    show_my_borrowed_books(reader)
    title = input("Podaj tytuł książki do przedłużenia: ")

    try:
        request = library.create_extension_request(reader, title)
    except ValueError as error:
        print(error)
    else:
        print(f"Wysłano prośbę o przedłużenie: {request.book.title}")


def show_all_loans(library):
    print("\nWSZYSTKIE WYPOŻYCZENIA")
    print("-" * 50)
    loans = library.list_current_loans()

    if len(loans) == 0:
        print("Brak aktualnych wypożyczeń.")
        return

    for index, (login, book) in enumerate(loans, start=1):
        print(f"{index}. {login}: {book.title} — {book.author}")


def handle_extension_requests(library):
    pending_requests = library.pending_extension_requests()

    print("\nPROŚBY O PRZEDŁUŻENIE")
    print("-" * 50)

    if len(pending_requests) == 0:
        print("Brak próśb do obsłużenia.")
        return

    for index, request in enumerate(pending_requests, start=1):
        print(f"{index}. {request.reader.login}: {request.book.title}")

    choice = input("Wybierz numer prośby: ")

    if not choice.isdigit():
        print("Niepoprawny numer prośby.")
        return

    decision = input("Zaakceptować prośbę? (t/n): ").lower()

    if decision not in ["t", "n"]:
        print("Niepoprawna decyzja.")
        return

    try:
        request = library.resolve_extension_request(
            int(choice) - 1,
            accepted=decision == "t",
        )
    except ValueError as error:
        print(error)
    else:
        print(f"Prośba została {request.status}.")


def reader_menu(library, reader):
    while True:
        show_menu(reader)
        choice = input("Wybierz opcję: ")

        if choice == "1":
            show_catalog(library)
        elif choice == "2":
            borrow_book_flow(library, reader)
        elif choice == "3":
            show_my_borrowed_books(reader)
        elif choice == "4":
            create_extension_request_flow(library, reader)
        elif choice == "5":
            print("Wylogowano. Do widzenia!")
            break
        else:
            print("Niepoprawny wybór. Spróbuj ponownie.")


def librarian_menu(library, librarian):
    while True:
        show_menu(librarian)
        choice = input("Wybierz opcję: ")

        if choice == "1":
            show_catalog(library)
        elif choice == "2":
            show_all_loans(library)
        elif choice == "3":
            handle_extension_requests(library)
        elif choice == "4":
            print("Wylogowano. Do widzenia!")
            break
        else:
            print("Niepoprawny wybór. Spróbuj ponownie.")


def main():
    library = create_initial_library()
    logged_user = login_user(library)

    if logged_user is None:
        return

    if isinstance(logged_user, Reader):
        reader_menu(library, logged_user)
    elif isinstance(logged_user, Librarian):
        librarian_menu(library, logged_user)


if __name__ == "__main__":
    main()
