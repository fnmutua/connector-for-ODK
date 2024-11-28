from qgis.PyQt.QtWidgets import QDialog, QVBoxLayout, QTextEdit
from PyQt5.QtCore import Qt

class ProgressFeedbackDialog(QDialog):
    """Dialog to display progress feedback."""
    
    def __init__(self):
        """Constructor."""
        super().__init__()
        self.setWindowTitle("Progress Feedback")
        self.setFixedSize(400, 300)

        # Layout for the dialog
        layout = QVBoxLayout()

        # Create a QTextEdit to show feedback
        self.feedback_text = QTextEdit(self)
        self.feedback_text.setReadOnly(True)
        self.feedback_text.setAlignment(Qt.AlignLeft)

        # Add the feedback text area to the layout
        layout.addWidget(self.feedback_text)

        # Set the layout
        self.setLayout(layout)

    def append_feedback(self, message):
        """Append message to the feedback text area."""
        self.feedback_text.append(message)
