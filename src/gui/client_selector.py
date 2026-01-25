"""Client selection dialog for QBO ToProcess."""

import tkinter as tk
from tkinter import ttk, messagebox
from typing import Optional, List, Dict


class ClientSelectorDialog(tk.Toplevel):
    """Dialog for selecting which clients to process."""

    def __init__(
        self,
        parent: Optional[tk.Tk] = None,
        clients: List[str] = None,
        client_status: Dict[str, bool] = None,
    ):
        """
        Initialize the client selector dialog.

        Args:
            parent: Parent window (optional)
            clients: List of client names
            client_status: Dict mapping client name to enabled status
        """
        self._own_root = False
        if parent is None:
            parent = tk.Tk()
            parent.withdraw()
            self._own_root = True

        super().__init__(parent)

        self.title("QBO ToProcess - Select Clients")
        self.resizable(False, False)

        self.clients = clients or []
        self.client_status = client_status or {}
        self.result: Optional[List[str]] = None

        # Checkbox variables
        self._checkboxes: Dict[str, tk.BooleanVar] = {}

        self._create_widgets()
        self._center_window()

        self.transient(parent)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

    def _create_widgets(self):
        """Create the dialog widgets."""
        main_frame = ttk.Frame(self, padding="20")
        main_frame.grid(row=0, column=0, sticky="nsew")

        # Title
        title_label = ttk.Label(
            main_frame,
            text="Select Clients to Process",
            font=("Segoe UI", 12, "bold"),
        )
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 15))

        # Instructions
        instr_label = ttk.Label(
            main_frame,
            text="Check the clients you want to export QBO data for:",
            font=("Segoe UI", 9),
        )
        instr_label.grid(row=1, column=0, columnspan=2, pady=(0, 10), sticky="w")

        # Client checkboxes
        clients_frame = ttk.LabelFrame(main_frame, text="Clients", padding="10")
        clients_frame.grid(row=2, column=0, columnspan=2, pady=10, sticky="ew")

        for i, client in enumerate(self.clients):
            var = tk.BooleanVar(value=self.client_status.get(client, True))
            self._checkboxes[client] = var

            cb = ttk.Checkbutton(
                clients_frame,
                text=client,
                variable=var,
            )
            cb.grid(row=i, column=0, sticky="w", pady=2)

        # Select All / None buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.grid(row=3, column=0, columnspan=2, pady=10)

        ttk.Button(
            btn_frame,
            text="Select All",
            command=self._select_all,
            width=12,
        ).grid(row=0, column=0, padx=5)

        ttk.Button(
            btn_frame,
            text="Select None",
            command=self._select_none,
            width=12,
        ).grid(row=0, column=1, padx=5)

        # Separator
        ttk.Separator(main_frame, orient="horizontal").grid(
            row=4, column=0, columnspan=2, sticky="ew", pady=10
        )

        # Action buttons
        action_frame = ttk.Frame(main_frame)
        action_frame.grid(row=5, column=0, columnspan=2)

        ttk.Button(
            action_frame,
            text="Process Selected",
            command=self._on_ok,
            width=15,
        ).grid(row=0, column=0, padx=10)

        ttk.Button(
            action_frame,
            text="Cancel",
            command=self._on_cancel,
            width=15,
        ).grid(row=0, column=1, padx=10)

    def _center_window(self):
        """Center the dialog on screen."""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f"+{x}+{y}")

    def _select_all(self):
        """Select all clients."""
        for var in self._checkboxes.values():
            var.set(True)

    def _select_none(self):
        """Deselect all clients."""
        for var in self._checkboxes.values():
            var.set(False)

    def _on_ok(self):
        """Handle OK button click."""
        selected = [
            client for client, var in self._checkboxes.items()
            if var.get()
        ]

        if not selected:
            messagebox.showwarning(
                "No Selection",
                "Please select at least one client to process.",
            )
            return

        self.result = selected
        self._close()

    def _on_cancel(self):
        """Handle Cancel button click."""
        self.result = None
        self._close()

    def _close(self):
        """Close the dialog."""
        self.grab_release()
        self.destroy()
        if self._own_root:
            self.master.destroy()


def select_clients(
    clients: List[str],
    client_status: Dict[str, bool] = None,
) -> Optional[List[str]]:
    """
    Show the client selection dialog.

    Args:
        clients: List of available client names
        client_status: Optional dict of client -> enabled status

    Returns:
        List of selected client names, or None if cancelled
    """
    root = tk.Tk()
    root.withdraw()

    dialog = ClientSelectorDialog(root, clients, client_status or {})
    root.wait_window(dialog)

    result = dialog.result
    root.destroy()

    return result


if __name__ == "__main__":
    # Test the dialog
    test_clients = ["BostonHCP", "LSC", "ELW", "SprayValet", "BosOpt"]
    result = select_clients(test_clients)
    if result:
        print(f"Selected: {result}")
    else:
        print("Cancelled")
