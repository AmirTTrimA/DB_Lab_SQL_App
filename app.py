import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException
import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="Bookstore Management API")


def get_db_connection():
    conn = None
    try:
        conn_str = os.getenv("DATABASE_URL2")
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        raise HTTPException(
            status_code=500, detail=f"Database connection error: {str(e)}"
        )
    finally:
        if conn is not None:
            conn.close()


# Endpoint to get the best salesman in Chicago for a specific year
@app.get("/salesman/best-in-chicago/{year}")
async def get_best_salesman_in_chicago(year: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.fn_BestSalesmanInChicago(?)", year)
        result = cursor.fetchone()
        if not result:
            raise HTTPException(
                status_code=404, detail="No salesmen found for the specified year"
            )
        return {
            "SalesmanID": result[0],
            "FirstName": result[1],
            "LastName": result[2],
            "NumberOfSales": result[3],
            "TotalSales": result[4],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get available stock for a specific book
@app.get("/books/{book_id}/availability")
async def get_available_stock_for_book(book_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.fn_GetAvailableStockForBook(?)", book_id)
        stocks = cursor.fetchall()
        return [
            {"ShopID": row[0], "ShopName": row[1], "AvailableStock": row[2]}
            for row in stocks
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get the price of a book
@app.get("/books/{book_id}/price")
async def get_book_price(book_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT dbo.fn_GetBookPrice(?)", book_id)
        price = cursor.fetchone()[0]
        return {"BookID": book_id, "Price": price}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get books by genre
@app.get("/books/genre/{genre}")
async def get_books_by_genre(genre: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.fn_GetBooksByGenre(?)", genre)
        books = cursor.fetchall()
        return [
            {
                "BookID": row[0],
                "Title": row[1],
                "Author": row[2],
                "Genre": row[3],
                "Price": row[4],
            }
            for row in books
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get the full name of a customer
@app.get("/customers/{customer_id}/full-name")
async def get_customer_full_name(customer_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT dbo.fn_GetCustomerFullName(?)", customer_id)
        full_name = cursor.fetchone()[0]
        return {"CustomerID": customer_id, "FullName": full_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get total number of books
@app.get("/books/total")
async def get_total_books():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT dbo.fn_GetTotalBooks()")
        total_books = cursor.fetchone()[0]
        return {"TotalBooks": total_books}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get total books sold by genre
@app.get("/books/genre/{genre}/total-sold")
async def get_total_books_sold_by_genre(genre: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT dbo.fn_GetTotalBooksSoldByGenre(?)", genre)
        total_sold = cursor.fetchone()[0]
        return {"Genre": genre, "TotalSold": total_sold}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get total purchases by customer
@app.get("/customers/{customer_id}/total-purchases")
async def get_total_purchases_by_customer(customer_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT dbo.fn_GetTotalPurchasesByCustomer(?)", customer_id)
        total_purchases = cursor.fetchone()[0]
        return {"CustomerID": customer_id, "TotalPurchases": total_purchases}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get total sales by shop
@app.get("/shops/{shop_id}/total-sales")
async def get_total_sales_by_shop(shop_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT dbo.fn_GetTotalSalesByShop(?)", shop_id)
        total_sales = cursor.fetchone()[0]
        return {"ShopID": shop_id, "TotalSales": total_sales}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get least sold books in each shop
@app.get("/shops/least-sold-books")
async def get_least_sold_books():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.fn_LeastSoldBooksInEachShop()")
        least_sold_books = cursor.fetchall()
        return [
            {"ShopName": row[0], "Title": row[1], "LeastSold": row[2]}
            for row in least_sold_books
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get most sold book by author
@app.get("/books/author/{author_name}/most-sold")
async def get_most_sold_book_by_author(author_name: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo.fn_MostSoldBookByAuthor(?)", author_name)
        result = cursor.fetchone()
        if not result:
            raise HTTPException(
                status_code=404, detail="No books found for the specified author"
            )
        return {"Title": result[0], "TotalSold": result[1]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get total sales by shop in a date range
@app.get("/shops/{shop_id}/total-sales/dates")
async def get_total_sales_by_shop_in_dates(
    shop_id: int, start_date: str, end_date: str
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM dbo.fn_TotalSalesByShopInDates(?, ?)", start_date, end_date
        )
        sales = cursor.fetchall()
        return [
            {"ShopID": row[0], "ShopName": row[1], "TotalSales": row[2]}
            for row in sales
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to add a new book
@app.post("/books/")
async def add_book(
    book_id: int, title: str, author: str, genre: str, price: float, published_date: str
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "EXEC dbo.pr_AddBook ?, ?, ?, ?, ?, ?",
            book_id,
            title,
            author,
            genre,
            price,
            published_date,
        )
        conn.commit()
        return {"message": "Book added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to add a sale
@app.post("/sales/")
async def add_sale(
    sale_id: int,
    shop_id: int,
    salesman_id: int,
    customer_id: int,
    sale_date: str,
    total_amount: float,
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "EXEC dbo.pr_AddSale ?, ?, ?, ?, ?, ?",
            sale_id,
            shop_id,
            salesman_id,
            customer_id,
            sale_date,
            total_amount,
        )
        conn.commit()
        return {"message": "Sale added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to delete a book
@app.delete("/books/{book_id}")
async def delete_book(book_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("EXEC dbo.pr_DeleteBook ?", book_id)
        conn.commit()
        return {"message": "Book deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to get customer purchase history
@app.get("/customers/{customer_id}/purchase-history")
async def get_customer_purchase_history(customer_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("EXEC dbo.pr_GetCustomerPurchaseHistory ?", customer_id)
        purchases = cursor.fetchall()
        return [
            {"SaleID": row[0], "SaleDate": row[1], "TotalAmount": row[2]}
            for row in purchases
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# Endpoint to update book price
@app.put("/books/{book_id}/price")
async def update_book_price(book_id: int, new_price: float):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("EXEC dbo.pr_UpdateBookPrice ?, ?", book_id, new_price)
        conn.commit()
        return {"message": "Book price updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
