import os
import json
import tkinter as tk
from tkinter import filedialog, ttk, messagebox, scrolledtext
from dbfread import DBF, FieldParser
from pymongo import MongoClient
from openai import OpenAI
import hashlib
import pickle
import markdown
from tkhtmlview import HTMLLabel

# Load OpenAI API key from file
key_file_path = os.path.join(os.path.dirname(__file__), "key")
with open(key_file_path, "r") as key_file:
    client = OpenAI(api_key=key_file.read().strip())

class CustomFieldParser(FieldParser):
    def parseN(self, field, data):
        """Parse numeric field (N)

        Returns int, float or None if the field is empty.
        """
        # In some files * is used for padding.
        data = data.strip().strip(b'*')

        try:
            return int(data)
        except ValueError:
            if not data.strip():
                return None
            elif data.strip() == b'.':
                return None
            else:
                # Account for , in numeric fields
                return float(data.replace(b',', b'.'))

class DBFMigratorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DBF to MongoDB Migration Tool")
        self.dbf_files = []
        self.selected_file = None
        self.preview_data = None
        self.schema_data = None
        self.cache_file = "gpt_cache.pkl"
        self.load_cache()

        # MongoDB Connection
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.db = self.mongo_client["dbf_migration"]

        # UI Layout
        self.create_widgets()

    def create_widgets(self):
        # Top Frame (Buttons)
        top_frame = tk.Frame(self.root)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        self.select_btn = tk.Button(top_frame, text="Select DBF Folder", command=self.load_dbf_files)
        self.select_btn.pack(side=tk.LEFT, padx=5)

        self.json_btn = tk.Button(top_frame, text="Export to JSON", command=self.export_to_json)
        self.json_btn.pack(side=tk.LEFT, padx=5)

        self.import_btn = tk.Button(top_frame, text="Import to MongoDB", command=self.import_to_mongo)
        self.import_btn.pack(side=tk.LEFT, padx=5)

        # Main Frame (Left & Right Panels)
        main_frame = tk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Left Panel (File List)
        left_panel = tk.Frame(main_frame, width=200)
        left_panel.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)

        self.file_listbox = tk.Listbox(left_panel, selectmode=tk.SINGLE)
        self.file_listbox.pack(fill=tk.BOTH, expand=True)
        self.file_listbox.bind("<<ListboxSelect>>", self.load_and_preview)

        # Right Panel (Preview & Schema)
        right_panel = tk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.preview_text = scrolledtext.ScrolledText(right_panel, wrap=tk.WORD, height=15)
        self.preview_text.pack(fill=tk.BOTH, expand=True, pady=5)

        self.schema_label = HTMLLabel(right_panel, html="<p>Schema Preview</p>")
        self.schema_label.pack(fill=tk.BOTH, expand=True, pady=5)

    def load_cache(self):
        try:
            with open(self.cache_file, "rb") as f:
                self.cache = pickle.load(f)
        except FileNotFoundError:
            self.cache = {}

    def load_dbf_files(self):
        directory = filedialog.askdirectory()
        if not directory:
            return

        self.dbf_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.lower().endswith(".dbf")]
        if not self.dbf_files:
            messagebox.showerror("Error", "No DBF files found in the selected directory.")
            return

        self.file_listbox.delete(0, tk.END)  # Clear previous entries
        for file_path in self.dbf_files:
            self.file_listbox.insert(tk.END, os.path.basename(file_path))  # Add to listbox

    def generate_summary(self, filename, sample_data):
        if not sample_data:
            return "No data available to summarize."

        prompt = (
            f"The following dataset is from the file {filename}. "
            f"Summarize its type: customer information, parts, suppliers, financial records, etc.\n\n"
            f"Sample:\n{json.dumps(sample_data, indent=4)}"
        )

        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant summarizing database records."},
                    {"role": "user", "content": prompt}
                ]
            )
            return response.choices[0].message.content.strip()

        except Exception as e:
            messagebox.showerror("OpenAI API Error", str(e))
            return "Error generating summary."

    def extract_schema_from_preview(self):
        """Extracts schema information by looking at the first few records."""
        if not self.preview_data:
            return {}

        return list(self.preview_data[0].keys()) if isinstance(self.preview_data, list) and self.preview_data else []

    def format_markdown(self, summary):
        """Convert markdown to HTML with better list handling."""
        return markdown.markdown(summary, extensions=['extra', 'nl2br'])

    def load_and_preview(self, event=None):
        selected_indices = self.file_listbox.curselection()
        if not selected_indices:
            return

        selected_filename = self.file_listbox.get(selected_indices[0])
        for file_path in self.dbf_files:
            if os.path.basename(file_path) == selected_filename:
                self.selected_file = file_path
                break

        self.preview_data = self.read_dbf(self.selected_file)
        self.schema_data = self.extract_schema_from_preview()

        
        # Take a sample of the preview_data from 3 records up to 10 and attempt to serialize it
        # if none of those subsets are serializable alert the user and update teh preview_text with the warning
        # and return. Use the subset that works to create the summary
        for i in range(3, 11):
            try:
                sample_data = json.dumps(self.preview_data[:i], indent=4, default=str)
                break
            except Exception as e:
                if i == 10:
                    messagebox.showerror("Error", f"Failed to serialize data: {e}")
                    self.preview_text.delete("1.0", tk.END)
                    self.preview_text.insert(tk.END, f"Failed to serialize data: {e}")
                    return



        # Generate summary using OpenAI API
        summary = self.get_cached_summary(self.selected_file)
        if not summary:
            summary = self.generate_summary(self.selected_file, sample_data)
            self.cache_summary(self.selected_file, summary)

        # Update UI with data preview
        self.preview_text.delete("1.0", tk.END)
        self.preview_text.insert(tk.END, json.dumps(self.preview_data, indent=4, default=str))

        # Combine schema data and markdown summary
        print("SUMMARY:")
        print(summary)
        print("################")
        summary = self.format_markdown(summary)

        summary = "Schema: " + ", ".join(self.schema_data) + "<br><br>" + summary

        # Update UI with schema preview and summary
        self.schema_label.set_html(summary)

    def read_dbf(self, filepath):
        try:
            table = DBF(filepath, parserclass=CustomFieldParser, load=True)
            return [dict(record) for record in table]
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read DBF file: {e}")
            return []

    def get_cached_summary(self, filename):
        file_hash = hashlib.md5(filename.encode()).hexdigest()
        return self.cache.get(file_hash, None)

    def cache_summary(self, filename, summary):
        file_hash = hashlib.md5(filename.encode()).hexdigest()
        self.cache[file_hash] = summary
        with open(self.cache_file, "wb") as f:
            pickle.dump(self.cache, f)

    def export_to_json(self):
        if not self.preview_data:
            messagebox.showwarning("Warning", "No data available to export.")
            return

        filepath = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files", "*.json")])
        if not filepath:
            return

        with open(filepath, "w") as json_file:
            json.dump(self.preview_data, json_file, indent=4, default=str)

        messagebox.showinfo("Success", "Data exported successfully!")

    def import_to_mongo(self):
        if not self.preview_data:
            messagebox.showwarning("Warning", "No data available to import.")
            return

        collection_name = os.path.basename(self.selected_file).split(".")[0].lower()
        collection = self.db[collection_name]
        collection.insert_many(self.preview_data)

        messagebox.showinfo("Success", f"Data imported into MongoDB collection: {collection_name}")

if __name__ == "__main__":
    root = tk.Tk()
    root.geometry("800x600")
    root.resizable(True, True)
    app = DBFMigratorApp(root)
    root.mainloop()
