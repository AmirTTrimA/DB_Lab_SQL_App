"""
app.py

This module implements a FastAPI application for managing a bookstore.
It provides endpoints for CRUD operations on books and stock management.
"""

import os
from datetime import date
from typing import List, Optional
from fastapi import FastAPI, HTTPException
import pyodbc
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator

# Clear any existing env variables and load new ones
if "DATABASE_URL" in os.environ:
    del os.environ["DATABASE_URL"]
load_dotenv(override=True)

# FastAPI app instance
app = FastAPI(title="Bookstore Management API")


def get_db_connection():
    """
    Establishes a connection to the database using the connection string
    from the environment variable 'DATABASE_URL'.

    Returns:
        conn: A connection object to the database.

    Raises:
        HTTPException: If there is an error connecting to the database.
    """
    try:
        conn_str = os.getenv("DATABASE_URL")
        print(f"Attempting to connect with: {conn_str}")  # For debugging
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to database: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Database connection error: {str(e)}"
        )


class Book(BaseModel):
    """
    Pydantic model representing a Book.

    Attributes:
        BookID (int): The unique identifier for the book.
        Title (str): The title of the book.
        Author (Optional[str]): The author of the book.
        Genre (Optional[str]): The genre of the book.
        Price (float): The price of the book.
        PublishedDate (Optional[date]): The publication date of the book.
    """

    BookID: int
    Title: str
    Author: Optional[str]
    Genre: Optional[str]
    Price: float
    PublishedDate: Optional[date]


class BookCreate(BaseModel):
    """
    Pydantic model for creating a new Book.

    Attributes:
        Title (str): The title of the book.
        Author (Optional[str]): The author of the book.
        Genre (Optional[str]): The genre of the book.
        Price (float): The price of the book, must be greater than zero.
        PublishedDate (Optional[date]): The publication date of the book.
    """

    Title: str = Field(..., max_length=255)
    Author: Optional[str] = Field(None, max_length=255)
    Genre: Optional[str] = Field(None, max_length=100)
    Price: float = Field(..., gt=0)
    PublishedDate: Optional[date] = None

    @validator("Price")
    def validate_price(cls, v):
        """
        Validates that the price is greater than zero.

        Args:
            cls: The class itself.
            v: The value of the price.

        Returns:
            float: The validated price rounded to two decimal places.

        Raises:
            ValueError: If the price is less than or equal to zero.
        """
        if v <= 0:
            raise ValueError("Price must be greater than zero")
        return round(float(v), 2)  # Ensure 2 decimal places


@app.get("/books/structure")
async def get_books_structure():
    """
    Retrieves the structure of the 'Books' table in the database.

    Returns:
        dict: A dictionary containing the table structure.

    Raises:
        HTTPException: If there is an error retrieving the table structure.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get table structure
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'Books'
            ORDER BY ORDINAL_POSITION
        """)

        columns = []
        for row in cursor.fetchall():
            columns.append(
                {
                    "column_name": row[0],
                    "data_type": row[1],
                    "max_length": row[2],
                    "nullable": row[3],
                }
            )

        return {"table_structure": columns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/books/", response_model=List[Book])
async def get_books(skip: int = 0, limit: int = 100):
    """
    Retrieves a list of books from the database with pagination.

    Args:
        skip (int): The number of records to skip (default is 0).
        limit (int): The maximum number of records to return (default is 100).

    Returns:
        List[Book]: A list of books.

    Raises:
        HTTPException: If there is an error retrieving the books.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT BookID, Title, Author, Genre, Price, PublishedDate FROM Books ORDER BY BookID OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            skip,
            limit,
        )
        books = []
        for row in cursor.fetchall():
            books.append(
                Book(
                    BookID=row[0],
                    Title=row[1],
                    Author=row[2],
                    Genre=row[3],
                    Price=float(row[4]),
                    PublishedDate=row[5],
                )
            )
        return books
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/books/{book_id}", response_model=Book)
async def get_book(book_id: int):
    """
    Retrieves a specific book by its ID.

    Args:
        book_id (int): The ID of the book to retrieve.

    Returns:
        Book: The requested book.

    Raises:
        HTTPException: If the book is not found or there is an error retrieving it.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT BookID, Title, Author, Genre, Price, PublishedDate FROM Books WHERE BookID = ?",
            book_id,
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Book not found")

        return Book(
            BookID=row[0],
            Title=row[1],
            Author=row[2],
            Genre=row[3],
            Price=float(row[4]),
            PublishedDate=row[5],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/books/", response_model=Book)
async def create_book(book: BookCreate):
    """
    Creates a new book in the database.

    Args:
        book (BookCreate): The book data to create.

    Returns:
        Book: The created book.

    Raises:
        HTTPException: If there is an error creating the book.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get the next BookID
        cursor.execute("SELECT ISNULL(MAX(BookID), 0) + 1 FROM Books")
        new_id = cursor.fetchone()[0]

        # Format the date as string if it exists
        published_date = (
            book.PublishedDate.strftime("%Y-%m-%d") if book.PublishedDate else None
        )

        # Prepare the insert statement
        sql = """
            INSERT INTO Books (BookID, Title, Author, Genre, Price, PublishedDate)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (
            new_id,
            book.Title,
            book.Author,
            book.Genre,
            book.Price,
            published_date,  # Use the formatted date string
        )

        # Print debug information
        print(f"Attempting to insert book with ID: {new_id}")
        print(f"Parameters: {params}")

        cursor.execute(sql, params)
        conn.commit()

        print("Book inserted successfully")

        return Book(
            BookID=new_id,
            Title=book.Title,
            Author=book.Author,
            Genre=book.Genre,
            Price=book.Price,
            PublishedDate=book.PublishedDate,
        )
    except Exception as e:
        print(f"Error creating book: {str(e)}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error creating book: {str(e)}")
    finally:
        if conn:
            conn.close()


@app.put("/books/{book_id}", response_model=Book)
async def update_book(book_id: int, book: BookCreate):
    """
    Updates an existing book in the database.

    Args:
        book_id (int): The ID of the book to update.
        book (BookCreate): The updated book data.

    Returns:
        Book: The updated book.

    Raises:
        HTTPException: If the book is not found or there is an error updating it.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if book exists
        cursor.execute("SELECT 1 FROM Books WHERE BookID = ?", book_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Book not found")

        # Prepare the update statement
        sql = """
            UPDATE Books 
            SET Title = ?, Author = ?, Genre = ?, Price = ?, PublishedDate = ?
            WHERE BookID = ?
        """
        params = (
            book.Title,
            book.Author if book.Author is not None else None,
            book.Genre if book.Genre is not None else None,
            book.Price,
            book.PublishedDate.strftime("%Y-%m-%d") if book.PublishedDate else None,
            book_id,
        )

        cursor.execute(sql, params)
        conn.commit()

        return Book(BookID=book_id, **book.dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.delete("/books/{book_id}")
async def delete_book(book_id: int):
    """
    Deletes a book from the database by its ID.

    Args:
        book_id (int): The ID of the book to delete.

    Returns:
        dict: A message indicating the book was deleted successfully.

    Raises:
        HTTPException: If the book is not found or there is an error deleting it.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if book exists
        cursor.execute("SELECT 1 FROM Books WHERE BookID = ?", book_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Book not found")

        cursor.execute("DELETE FROM Books WHERE BookID = ?", book_id)
        conn.commit()

        return {"message": "Book deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/stock/{shop_id}")
async def get_shop_stock(shop_id: int):
    """
    Retrieves the stock information for a specific shop.

    Args:
        shop_id (int): The ID of the shop to retrieve stock for.

    Returns:
        List[dict]: A list of stock items with titles and quantities.

    Raises:
        HTTPException: If there is an error retrieving the stock information.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT b.Title, s.Quantity, b.BookID
            FROM Stocks s
            JOIN Books b ON s.BookID = b.BookID
            WHERE s.ShopID = ?
        """,
            shop_id,
        )

        stock = []
        for row in cursor.fetchall():
            stock.append({"title": row[0], "quantity": row[1], "book_id": row[2]})
        return stock
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
