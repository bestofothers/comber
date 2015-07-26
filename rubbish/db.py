import MySQLdb as mdb

# !!! FIX: documentation
# !!! BUG: insertion of Null
# !!! BUG: Fix MySQL duplicate error - maybe it would help solve problem
# !!! FIX: Complete email extraction
# !!! FIX: Remove unwanted sites


class FatalException(Exception):
    pass


class DB(object):

    """
    Store data to database from Scraper.
    """

    def __init__(self, query, search_source):

        """Initialisation of DB

        @:param query - term/keyword passed to search engine or directory
        @:param search_source - search engine or directory
        """
        self._query = query.lower().encode('utf-8')
        self._search_source = search_source.lower().encode('utf-8')
        self._con = mdb.connect('127.0.0.1', 'root', 'root', 'scraper')
        self._cursor = self._con.cursor()

    def store_data(self, title, desc, webs, emails):
        """Public for storing full company data
        NOTE: To be improved to store other fields that are null but available
        for other search engines like alibaba, made-in-china

        @:param title -- name (alibaba) or title of (google)
        @:param desc -- google description of site null of not google or bing
        @:param webs -- list of websites
        """
        try:
            qry_id = self._select_tbl_qs()

            for web in list(set(webs)):
                web_id = self._select_web(web.encode('utf-8'))
                if web_id:
                    for email in emails:
                        r = self._select_email(email)
                        if r:
                            print("Email Already Exists!!!")
                        else:
                            self._insert_email(web_id[0], email)
                else:
                    last_id = self._select_company(title.encode('utf-8'), desc.encode('utf-8'), qry_id)
                    web_id = self._insert_web(last_id, web.encode('utf-8'))
                    for email in emails:
                        r = self._select_email(email)
                        if r:
                            print("Email Already Exists!!!")
                        else:
                            self._insert_email(web_id, email)

        except mdb.Error as e:
            print("[Error] {}: {}".format(*e.args))
        finally:
            self._cursor.close()
            self._con.close()

    def _insert_email(self, web_id, email):

        """Insert a query and returns inserted id."""
        print(web_id, email)

        sql = "INSERT INTO emails(website_id, email) VALUES(%s, %s)"
        try:
            self._cursor.execute(sql, (web_id, email))
            self._con.commit()
        except:
            self._con.rollback()
            raise

    def _select_email(self, email):

        """Insert a query and returns inserted id."""

        sql = "SELECT email FROM emails WHERE email = %s "
        try:
            self._cursor.execute(sql, (email,))
            result = self._cursor.fetchone()
        except:
            raise
        return result



    def _select_tbl_qs(self):
        sql = "SELECT tbl_qs.query_id, tbl_qs.source_id, search_site.id, search_site.source, search_query.query " \
              "FROM tbl_qs LEFT JOIN search_site ON tbl_qs.source_id = search_site.id LEFT JOIN search_query " \
              "ON search_query.id = tbl_qs.query_id WHERE search_query.query = %s " \
              "AND search_site.source = %s"
        try:
            self._cursor.execute(sql, (self._query, self._search_source))
            result = self._cursor.fetchone()
        except:
            raise

        if result:
            return result[0]
        else:
            source_id = self._select_source()
            if source_id:
                query_id = self._select_query()
                if query_id:
                    self._insert_tbl_qs(query_id, source_id)
                else:
                    raise FatalException("Fatal Error 1")
            else:
                raise FatalException("Fatal Error 2")
            return query_id

    def _insert_tbl_qs(self, qi, si):

        sql = "INSERT INTO tbl_qs(query_id, source_id) VALUES(%s, %s) "
        try:
            self._cursor.execute(sql, (qi, si))
            self._con.commit()
        except:
            self._con.rollback()
            raise

    def _insert_company(self, title, desc, qry):

        """Insert a company and return the id.

        @:param title -- name (alibaba) or title of (google)
        @:param desc -- google description of site null of not google or bing
        @:param qry -- id of query that returned the company info
        """

        sql = "INSERT INTO company(title, description, query_id) VALUES(%s, %s, %s)"
        try:
            self._cursor.execute(sql, (title, desc, qry))
            self._con.commit()
        except:
            self._con.rollback()
            raise
        return self._cursor.lastrowid

    def _select_company(self, title, desc, qry):

        sql = "SELECT id FROM company WHERE title = %s AND description = %s"
        try:
            self._cursor.execute(sql, (title, desc))
            result = self._cursor.fetchone()
        except:
            raise
        if result:
            return result[0]
        else:
            return self._insert_company(title, desc, qry)

    def _select_query(self):

        """Returns source_id of a query - depends on other functions if not found."""

        sql = "SELECT id FROM search_query WHERE query = %s"
        try:
            self._cursor.execute(sql, (self._query,))
            result = self._cursor.fetchone()
        except:
            raise

        if result:
            return result[0]
        else:
            return self._insert_query()

    def _insert_query(self):

        """Insert a query and returns inserted id."""

        sql = "INSERT INTO search_query(query) VALUES(%s) "
        try:
            self._cursor.execute(sql, (self._query,))
            self._con.commit()
        except:
            self._con.rollback()
            raise

        return self._cursor.lastrowid

    def _insert_web(self, company_id, web):

        """Insert website of a company and close connection.

        @:param company_id -- id of the company
        @:param web -- website
        """

        sql = "INSERT INTO website(company_id, website) VALUES(%s, %s)"
        try:
            self._cursor.execute(sql, (company_id, web))
            self._con.commit()
        except:
            self._con.rollback()
            raise

        return self._cursor.lastrowid

    def _select_web(self, web):

        """Get website and return tuple of url and company_id.

        @:param web -- get website
        """

        sql = "SELECT id, company_id FROM website WHERE website = %s"
        try:
            self._cursor.execute(sql, (web,))
            result = self._cursor.fetchone()
        except:
            raise

        return result

    def _select_source(self):

        """returns id of the search engine used for search query."""

        sql = "SELECT id FROM search_site WHERE source = %s"
        try:
            self._cursor.execute(sql, (self._search_source,))
            result = self._cursor.fetchone()
        except:
            raise
        if result:
            return result[0]
        else:
            return self._insert_source()

    def _insert_source(self):

        """Insert search engine source and return its id."""

        sql = "INSERT INTO search_site(source) VALUES(%s) "
        try:
            self._cursor.execute(sql, (self._search_source,))
            self._con.commit()
        except mdb.Error:
            self._con.rollback()
            raise
        return self._cursor.lastrowid
