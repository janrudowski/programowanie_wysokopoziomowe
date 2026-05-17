# Biblioteka — rozszerzenie funkcyjne
# Programowanie funkcyjne: lambda, filter, map, sorted, comprehensions i funkcje wyższego rzędu


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
    def borrowed_count(self):
        return self._total_copies - self._available_copies

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
        self._reservations = []

    @property
    def borrowed_books(self):
        return list(self._borrowed_books)

    @property
    def extension_requests(self):
        return list(self._extension_requests)

    @property
    def reservations(self):
        return list(self._reservations)

    def add_borrowed_book(self, book):
        self._borrowed_books.append(book)

    def remove_borrowed_book(self, book):
        self._borrowed_books.remove(book)

    def has_borrowed(self, book):
        return book in self._borrowed_books

    def add_extension_request(self, request):
        self._extension_requests.append(request)

    def add_reservation(self, reservation):
        self._reservations.append(reservation)

    def menu_options(self):
        return [
            "Przeglądaj katalog",
            "Filtruj katalog",
            "Sortuj katalog",
            "Wypożycz książkę",
            "Moje wypożyczenia",
            "Poproś o przedłużenie",
            "Zarezerwuj niedostępną książkę",
            "Wyloguj",
        ]


class Librarian(User):
    def __init__(self, login, password):
        super().__init__(login, password, "bibliotekarz")

    def menu_options(self):
        return [
            "Przeglądaj katalog",
            "Filtruj katalog",
            "Sortuj katalog",
            "Lista wszystkich wypożyczeń",
            "Obsługa próśb o przedłużenie",
            "Statystyki",
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


class Reservation:
    def __init__(self, reader, book):
        self.reader = reader
        self.book = book
        self.status = "aktywna"

    def __str__(self):
        return f"{self.reader.login} — {self.book.title} ({self.status})"


class Library:
    def __init__(self, books=None, users=None):
        self._books = list(books) if books is not None else []
        self._users = list(users) if users is not None else []
        self._extension_requests = []
        self._reservations = []

    @property
    def books(self):
        return list(self._books)

    @property
    def users(self):
        return list(self._users)

    @property
    def reservations(self):
        return list(self._reservations)

    def add_book(self, book):
        self._books.append(book)

    def add_user(self, user):
        self._users.append(user)

    def authenticate(self, login, password):
        return next(
            filter(
                lambda user: user.login == login and user.authenticate(password),
                self._users,
            ),
            None,
        )

    def find_book_by_title(self, title):
        searched_title = title.lower()
        return next(
            filter(lambda book: book.title.lower() == searched_title, self._books),
            None,
        )

    def select_books(self, predicate):
        """Funkcja wyższego rzędu: kryterium wybierania przekazuje wywołujący."""
        return list(filter(predicate, self._books))

    def filter_catalog(self, phrase="", only_available=False):
        normalized_phrase = phrase.strip().lower()
        by_phrase = lambda book: (
            normalized_phrase == ""
            or normalized_phrase in book.title.lower()
            or normalized_phrase in book.author.lower()
        )
        by_availability = lambda book: not only_available or book.available_copies > 0

        return self.select_books(
            lambda book: by_phrase(book) and by_availability(book)
        )

    def sort_catalog(self, sort_by):
        sort_keys = {
            "title": lambda book: book.title.lower(),
            "author": lambda book: book.author.lower(),
            "available_copies": lambda book: book.available_copies,
        }

        if sort_by not in sort_keys:
            raise ValueError("Niepoprawne kryterium sortowania.")

        return sorted(self._books, key=sort_keys[sort_by])

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
        return [
            (reader.login, book)
            for reader in filter(lambda user: isinstance(user, Reader), self._users)
            for book in reader.borrowed_books
        ]

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
        return list(
            filter(lambda request: request.status == "oczekująca", self._extension_requests)
        )

    def reserve_book(self, reader, title):
        if not isinstance(reader, Reader):
            raise ValueError("Tylko czytelnik może rezerwować książki.")

        book = self.find_book_by_title(title)

        if book is None:
            raise ValueError("Nie znaleziono książki o podanym tytule.")

        if book.available_copies > 0:
            raise ValueError("Tę książkę można wypożyczyć, nie trzeba jej rezerwować.")

        existing = list(
            filter(
                lambda reservation: (
                    reservation.reader == reader
                    and reservation.book == book
                    and reservation.status == "aktywna"
                ),
                self._reservations,
            )
        )

        if len(existing) > 0:
            raise ValueError("Masz już aktywną rezerwację tej książki.")

        reservation = Reservation(reader, book)
        reader.add_reservation(reservation)
        self._reservations.append(reservation)
        return reservation

    def reservations_for_book(self, book):
        return list(
            filter(
                lambda reservation: (
                    reservation.book == book and reservation.status == "aktywna"
                ),
                self._reservations,
            )
        )

    def extension_requests_with_reservation_info(self):
        return [
            {
                "request": request,
                "reservation_count": len(self.reservations_for_book(request.book)),
                "has_reservations": len(self.reservations_for_book(request.book)) > 0,
            }
            for request in self.pending_extension_requests()
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

    def statistics(self):
        readers = list(filter(lambda user: isinstance(user, Reader), self._users))
        active_loans_by_reader = {
            reader.login: len(reader.borrowed_books)
            for reader in readers
        }
        borrowed_counts = list(map(lambda reader: len(reader.borrowed_books), readers))
        most_popular = sorted(
            self._books,
            key=lambda book: (-book.borrowed_count, book.title.lower()),
        )[0] if len(self._books) > 0 else None

        return {
            "most_popular_book": most_popular,
            "active_loans_count": sum(borrowed_counts),
            "readers_by_borrowed_count": sorted(
                readers,
                key=lambda reader: len(reader.borrowed_books),
                reverse=True,
            ),
            "active_loans_by_reader": active_loans_by_reader,
        }


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


def show_books(title, books):
    print(f"\n{title}")
    print("-" * 60)

    if len(books) == 0:
        print("Brak książek do wyświetlenia.")
        return

    for index, book in enumerate(books, start=1):
        print(f"{index}. {book}")


def show_catalog(library):
    show_books("KATALOG KSIĄŻEK", library.books)


def show_menu(user):
    print("\nMENU")

    for index, option in enumerate(user.menu_options(), start=1):
        print(f"{index}. {option}")


def filter_catalog_flow(library):
    phrase = input("Podaj frazę z tytułu lub autora (Enter = dowolna): ")
    only_available_choice = input("Pokazać tylko dostępne książki? (t/n): ").lower()
    only_available = only_available_choice == "t"
    books = library.filter_catalog(phrase, only_available)
    show_books("WYNIKI FILTROWANIA", books)


def sort_catalog_flow(library):
    print("\nSORTOWANIE")
    print("1. Tytuł")
    print("2. Autor")
    print("3. Liczba dostępnych sztuk")
    choice = input("Wybierz kryterium: ")
    criteria = {
        "1": "title",
        "2": "author",
        "3": "available_copies",
    }

    if choice not in criteria:
        print("Niepoprawne kryterium sortowania.")
        return

    show_books("KATALOG POSORTOWANY", library.sort_catalog(criteria[choice]))


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
    print("-" * 60)

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


def reserve_book_flow(library, reader):
    unavailable_books = library.filter_catalog(only_available=False)
    unavailable_books = list(filter(lambda book: book.available_copies == 0, unavailable_books))
    show_books("KSIĄŻKI NIEDOSTĘPNE DO REZERWACJI", unavailable_books)

    if len(unavailable_books) == 0:
        return

    title = input("Podaj tytuł książki do rezerwacji: ")

    try:
        reservation = library.reserve_book(reader, title)
    except ValueError as error:
        print(error)
    else:
        print(f"Zarezerwowano książkę: {reservation.book.title}")


def show_all_loans(library):
    print("\nWSZYSTKIE WYPOŻYCZENIA")
    print("-" * 60)
    loans = library.list_current_loans()

    if len(loans) == 0:
        print("Brak aktualnych wypożyczeń.")
        return

    for index, (login, book) in enumerate(loans, start=1):
        print(f"{index}. {login}: {book.title} — {book.author}")


def handle_extension_requests(library):
    requests_info = library.extension_requests_with_reservation_info()

    print("\nPROŚBY O PRZEDŁUŻENIE")
    print("-" * 60)

    if len(requests_info) == 0:
        print("Brak próśb do obsłużenia.")
        return

    for index, info in enumerate(requests_info, start=1):
        request = info["request"]
        reservation_info = (
            f"rezerwacje: {info['reservation_count']}"
            if info["has_reservations"]
            else "brak rezerwacji"
        )
        print(f"{index}. {request.reader.login}: {request.book.title} ({reservation_info})")

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


def show_statistics(library):
    stats = library.statistics()
    most_popular = stats["most_popular_book"]

    print("\nSTATYSTYKI")
    print("-" * 60)

    if most_popular is None:
        print("Brak książek w bibliotece.")
    else:
        print(
            "Najpopularniejsza książka: "
            f"{most_popular.title} — wypożyczone sztuki: {most_popular.borrowed_count}"
        )

    print(f"Liczba aktywnych wypożyczeń ogółem: {stats['active_loans_count']}")
    print("\nCzytelnicy wg liczby wypożyczonych książek:")

    for index, reader in enumerate(stats["readers_by_borrowed_count"], start=1):
        print(f"{index}. {reader.login}: {len(reader.borrowed_books)}")


def reader_menu(library, reader):
    while True:
        show_menu(reader)
        choice = input("Wybierz opcję: ")

        if choice == "1":
            show_catalog(library)
        elif choice == "2":
            filter_catalog_flow(library)
        elif choice == "3":
            sort_catalog_flow(library)
        elif choice == "4":
            borrow_book_flow(library, reader)
        elif choice == "5":
            show_my_borrowed_books(reader)
        elif choice == "6":
            create_extension_request_flow(library, reader)
        elif choice == "7":
            reserve_book_flow(library, reader)
        elif choice == "8":
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
            filter_catalog_flow(library)
        elif choice == "3":
            sort_catalog_flow(library)
        elif choice == "4":
            show_all_loans(library)
        elif choice == "5":
            handle_extension_requests(library)
        elif choice == "6":
            show_statistics(library)
        elif choice == "7":
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
