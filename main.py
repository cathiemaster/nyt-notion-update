import requests
import json
import pandas as pd
import constants

def get_notion_block_children(block_id):
    URL = f"https://api.notion.com/v1/blocks/{block_id}/children"

    try:
        resp = requests.get(
            url=URL,
            headers={
                "Authorization": f"Bearer {constants.NOTION_AUTH}",
                "Accept": "*/*",
                "Notion-Version": constants.NOTION_VERSION
            }
        )

        if resp.status_code == 200:
            content = json.loads(resp.content)
            return content
        
        return None
    except Exception as ex:
        print(ex)

def patch_notion_block_children(block_id, data):
    URL = f"https://api.notion.com/v1/blocks/{block_id}/children"

    try:
        api_data = {"children": data}
        api_data_str = json.dumps(api_data)
        # print(api_data_str)

        resp = requests.patch(
            URL,
            data=json.dumps(api_data),
            headers={
                "Authorization": f"Bearer {constants.NOTION_AUTH}",
                "Accept": "*/*",
                "Notion-Version": constants.NOTION_VERSION,
                "Content-Type": "application/json"
            }
        )

        return resp.status_code
    except Exception as ex:
        print(ex)

def get_google_book_entry(title, author_last_name=None): 
    URL = f"https://www.googleapis.com/books/v1/volumes?q={title}"
    URL = URL + f"+inauthor:{author_last_name}&key={constants.GOOGLE_BOOKS_AUTH}" if author_last_name else URL + f"&key={constants.GOOGLE_BOOKS_AUTH}"

    try:
        resp = requests.get(
            url=URL,
            headers={
                "Accept": "*/*"
            }
        )

        if resp.status_code == 200:
            content = json.loads(resp.content)
            return content
        
        return None
    except Exception as ex:
        print(ex)

    
def process_book_list(block_id): 
    books_block = get_notion_block_children(block_id)
    books_df = pd.json_normalize(books_block['results'], record_path=['bulleted_list_item', ['rich_text']])

    if not books_df.empty:
        books_df[['title', 'author']] = books_df['plain_text'].str.split(' - ', n=1, expand=True)
        books_df = books_df[['title', 'author']]
    else:
        books_df = pd.DataFrame(columns=['title', 'author'])

    return books_df


def is_book_completed(completed_books, title, author=""):
    return title in completed_books['title']

def is_book_listed(tbr_fiction, tbr_nonfiction, tbr_to_be_catalogued, title, author=""):
    return (title in tbr_fiction['title']) and (title in tbr_nonfiction['title']) and (title in tbr_to_be_catalogued['title'])


def main():
    print("------------ starting script ------------")
    completed_books_df = process_book_list(constants.COMPLETED_BOOKS_BLOCK_ID)
    tbr_fiction_df = process_book_list(constants.FICTION_BLOCK_ID)
    tbr_nonfiction_df = process_book_list(constants.NONFICTION_MEMOIRS_BLOCK_ID)
    # tbr_classics_df = process_book_list(CLASSICS_BLOCK_ID)
    # tbr_graphic_novels_df = process_book_list(GRAPHIC_NOVELS_BLOCK_ID)
    tbr_to_be_catalogued_df = process_book_list(constants.TO_BE_CATALOGUED_BLOCK_ID)

    tbr_fiction_data = []
    tbr_nonfiction_data = []
    tbr_catalogued_data = []

    book_count = 0

    nyt_best_books_df = pd.read_csv(constants.NYT_BEST_BOOKS_FULL_PATH)
    nyt_best_books_df.columns = ['title', 'author']
    nyt_best_books_df = nyt_best_books_df[['title', 'author']].fillna("")

    for ind, row in nyt_best_books_df.iterrows():
        book_count += 1

        title = row["title"]
        author = row["author"] if row["author"] != "" else ""

        if (is_book_completed(completed_books_df, title, author) or (is_book_listed(tbr_fiction_df, tbr_nonfiction_df, tbr_to_be_catalogued_df, title, author))):
            print(f"{title} has already been processed")
            continue

        title_formatted = row["title"].replace(" ", "").lower()
        author_last_name = row["author"].split(" ")[-1].lower() if row["author"] != "" else None

        google_book_entry = get_google_book_entry(title_formatted, author_last_name)
        if not google_book_entry or google_book_entry['totalItems'] == 0:
            if title not in tbr_to_be_catalogued_df['title']:
                tbr_catalogued_data.append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{
                            "type": "text",
                            "text": {
                                "content": title + " - " + author
                            }, 
                            "plain_text": title + " - " + author
                        }]
                    }
                })

                tbr_to_be_catalogued_df.loc[len(tbr_to_be_catalogued_df)] = [title, author]
                print(f"no google books entry found for {title}, cataloguing")
            else:
                print(f"no google books entry found for {title}, already catalogued")

            continue

        google_books_unfiltered_df = pd.json_normalize(google_book_entry['items']).fillna('')
        if ('volumeInfo.title' in google_books_unfiltered_df.columns) and ('volumeInfo.authors' in google_books_unfiltered_df.columns):
    
            google_books_df = google_books_unfiltered_df[['volumeInfo.title', 'volumeInfo.authors', 'volumeInfo.publishedDate']].rename(columns={"volumeInfo.title": "title", "volumeInfo.authors": "authors", "volumeInfo.publishedDate": "published_date"})
            
            google_books_df['categories'] = google_books_unfiltered_df['volumeInfo.categories'] if 'volumeInfo.categories' in google_books_unfiltered_df.columns else ''

            google_books_df['authors'] = [",".join(map(str, a)) for a in google_books_df['authors']]
            google_books_df['categories'] = [",".join(map(str, a)) for a in google_books_df['categories']]

            query = "title == @title and @author in authors" if author != "" else "title == @title"
            google_books_df.query(query, inplace=True)
            google_books_df = google_books_df.sort_values(by=['published_date']).drop_duplicates()

            if google_books_df.empty:
                tbr_catalogued_data.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{
                                "type": "text",
                                "text": {
                                    "content": title + " - " + author
                                }, 
                                "plain_text": title + " - " + author
                            }]
                        }
                    })

                tbr_to_be_catalogued_df.loc[len(tbr_to_be_catalogued_df)] = [title, author]
                print(f"processed {title} - {author}\tdid not find google books records")
                continue

            # take earliest published text
            genre = google_books_df['categories'].iloc[0]
            if (genre.lower() in ["fiction"]) and (title not in tbr_fiction_df['title']):
                tbr_fiction_df.loc[len(tbr_fiction_df)] = [title, author]
                tbr_fiction_data.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{
                                "type": "text",
                                "text": {
                                    "content": title + " - " + author
                                }, 
                                "plain_text": title + " - " + author
                            }]
                        }
                    })
                
            elif (genre.lower() in ["nonfiction", "non-fiction", "biography", "autobiography", "biography & autobiography"]) and (title not in tbr_nonfiction_df):
                tbr_nonfiction_df.loc[len(tbr_nonfiction_df)] = [title, author]
                tbr_nonfiction_data.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{
                                "type": "text",
                                "text": {
                                    "content": title + " - " + author
                                }, 
                                "plain_text": title + " - " + author
                            }]
                        }
                    })
            else:
                tbr_to_be_catalogued_df.loc[len(tbr_to_be_catalogued_df)] = [title, author]
                tbr_catalogued_data.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{
                                "type": "text",
                                "text": {
                                    "content": title + " - " + author
                                }, 
                                "plain_text": title + " - " + author
                            }]
                        }
                    })

            print(f"processed {title} - {author} - {genre}")
        else:
            tbr_catalogued_data.append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{
                            "type": "text",
                            "text": {
                                "content": title + " - " + author
                            }, 
                            "plain_text": title + " - " + author
                        }]
                    }
                })
            
            tbr_to_be_catalogued_df.loc[len(tbr_to_be_catalogued_df)] = [title, author]
            print(f"no google books entry found for {title}, cataloguing")
            continue

        if book_count == 1:
            break

    print("------------ updating notion ------------")
    tbr_fiction_status = patch_notion_block_children(constants.FICTION_BLOCK_ID, tbr_fiction_data)
    tbr_nonfiction_status = patch_notion_block_children(constants.NONFICTION_MEMOIRS_BLOCK_ID, tbr_nonfiction_data)
    tbr_cataloged_status = patch_notion_block_children(constants.TO_BE_CATALOGUED_BLOCK_ID, tbr_catalogued_data)

    print(f"tbr_fiction status: {tbr_fiction_status}\ntbr_nonfiction status: {tbr_nonfiction_status}\ntbr_to_be_catalogued status: {tbr_cataloged_status}")

    print("------------ finished script ------------")


if __name__ == "__main__":
    main()