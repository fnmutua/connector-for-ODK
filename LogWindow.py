from PyQt5.QtWidgets import QDialog, QVBoxLayout, QTextEdit, QPushButton

class LogWindow(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Log Window")
        
        # Create layout for the log window
        layout = QVBoxLayout()

        # Create a QTextEdit widget to show logs
        self.log_text = QTextEdit(self)
        self.log_text.setReadOnly(True)  # Make it read-only
        layout.addWidget(self.log_text)

        # Add close button to the log window
        self.close_button = QPushButton("Close")
        self.close_button.clicked.connect(self.close)
        layout.addWidget(self.close_button)

        # Set layout for the dialog
        self.setLayout(layout)

    def append_log(self, message):
        """ Append a log message to the text area """
        self.log_text.append(message)
        # Auto scroll to the bottom to show the latest logs
        self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())

    def clear_log(self):
        """ Clear the log window """
        self.log_text.clear()
