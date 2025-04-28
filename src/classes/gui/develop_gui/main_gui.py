import sys
from PySide6.QtWidgets import QApplication
from app.database import DatabaseManager
from app.widgets.table_viewer import TableViewerWindow


def main():
    app = QApplication(sys.argv)

    # Инициализация БД
    db = DatabaseManager()
    if not db.connect():
        sys.exit(1)

    # Главное окно
    window = TableViewerWindow(db)
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()