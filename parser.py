import undetected_chromedriver as uc
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import datetime
from database import connect, create_tables
import hashlib
from time import sleep
from random import SystemRandom
import logging
from base64 import b64decode
import secrets
import re
import os


# инициализируем базу, если ее еще нет
create_tables()


STATE0 = "state0"
STATE1 = "state1"
STATE2 = "state2"
CLEANUP = "cleanup"


class ParserError(Exception):
    pass


CITILINK_URL = "https://www.citilink.ru"
CITILINK_CATALOG_URL = CITILINK_URL + "/catalog/"
CITILINK_IMAGE_URL_REGEX = r'https://items\.s1\.citilink\.ru/.+\.jpg'

PATH_TO_IMAGES = "images"
PATH_TO_SCREENSHOTS = "errors"

STATE1_SLEEP_SECONDS = 5.
STATE2_SLEEP_SECONDS = 20.


# настраиваем, чтобы логи писались в файл и в консоль
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


def catch_images(message):
    response_info = message["params"]["response"]
    response_headers = response_info["headers"]
    response_url = response_info["url"]
    # ищем только изображения
    if re.fullmatch(CITILINK_IMAGE_URL_REGEX, response_url) and response_headers["content-type"] == "image/jpeg":
        try:
            request_id = message["params"]["requestId"]
            # найдем "айди" изображения
            image_id = hashlib.sha256(response_url.encode("utf-8")).hexdigest()
            filename = f"{image_id}.jpg"
            path_to_image = os.path.join(PATH_TO_IMAGES, filename)
            response_body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})["body"]
            image_record = {"url": response_url, "path": filename}
            # сохраним изображение в файл на диске
            with open(path_to_image, "wb") as image_file:
                image_file.write(b64decode(response_body))
            # теперь сообщим об этом изображении базе данных
            db = connect()
            cur = db.cursor()
            # добавим изображение в базу
            # если изображение уже есть в базе, то просто назначим путь до изображения на диске
            cur.execute(
                """
INSERT INTO
    item_image (item_id, url, path, number)
VALUES
    (NULL, :url, :path, -1)
ON CONFLICT DO
    UPDATE
        SET path = :path
;
                    """,
                image_record
            )
            db.commit()
            cur.close()
            db.close()
        except WebDriverException as ex:
            logging.error(f"{type(ex)}, {ex}")
        except Exception as ex:
            logging.error(f"{type(ex)}, {ex}")


def sleep_about(x):
    sleep(x + random.random() * random.choice([-1, 1]))


driver = uc.Chrome(enable_cdp_events=True)
driver.add_cdp_listener(
    "Network.responseReceived",
    catch_images
)
driver.maximize_window()


random = SystemRandom()


def take_screenshot():
    filename = f"{secrets.token_hex()}.png"
    path = os.path.join(PATH_TO_SCREENSHOTS, filename)
    driver.save_screenshot(path)
    return filename


def state0():
    """
    Это начальное состояние парсера, когда еще ничего неизвестно о дереве.
    Запускать, если еще ни разу не запускался парсер или парсится в новую базу данных.
    :return:
    """
    with open("last_state", "w") as state_file:
        state_file.write(STATE0)

    db = connect()
    driver.get(CITILINK_CATALOG_URL)
    # парсим каталог на кнопки перехода по категориям
    try:
        # ждем 10 секунд до появления нужного элемента
        parent_element = WebDriverWait(driver, 10).until(
            expected_conditions.presence_of_element_located((By.CSS_SELECTOR, "div.CatalogLayout__content"))
        )
    except TimeoutException as ex:
        logging.error(
            f"Родительский элемент не найден на {CITILINK_CATALOG_URL}. "
            f"Скорее всего, изменилась структура сайта и парсер больше не работает. Ошибка: {ex}"
        )
        driver.quit()
        raise ParserError()

    # после этого ищем категории
    category_containers = parent_element.find_elements(By.CSS_SELECTOR, "div.CatalogLayout__item-title-wrapper")
    for category_container in category_containers:
        name = category_container.find_element(By.CSS_SELECTOR, "span.CatalogLayout__category-title").text
        url = category_container.find_element(By.CSS_SELECTOR, "a.CatalogLayout__link_level-1").get_attribute("href")
        id = hashlib.sha256(url.encode("utf-8")).hexdigest()
        category = {"id": id, "name": name, "url": url}

        cur = db.cursor()
        cur.execute(
            """
INSERT INTO 
    category (id, name, url, is_traversed, is_leaf, parent_category_id) 
VALUES 
    (:id, :name, :url, 0, 1, NULL) 
            """, category
        )
        db.commit()
        cur.close()

        subcategory_links = category_container.find_elements(By.CSS_SELECTOR, "a.CatalogLayout__item-link")
        for subcategory_link in subcategory_links:
            url = subcategory_link.get_attribute("href")
            name = subcategory_link.text.strip()
            subcategory = {
                "id": hashlib.sha256(url.encode("utf-8")).hexdigest(),
                "name": name, "url": url, "parent_category_id": id
            }
            logging.info(
                f"Подкатегория: name='{subcategory['name']}', url={subcategory['url']}, id={subcategory['id']}"
            )

            cur = db.cursor()
            # из-за того, что в категориях могут повторяться подкатегории, мы ничего не делаем при повторке
            cur.execute(
                """
INSERT INTO 
    category (id, name, url, is_traversed, is_leaf, parent_category_id) 
VALUES 
    (:id, :name, :url, 0, NULL, :parent_category_id) 
ON CONFLICT
DO
    NOTHING
                """,
                subcategory
            )
            db.commit()
            cur.close()
    db.close()


def state1():
    """
    Запускать, когда нужно исследовать дерево на предмет листьев (то есть дойти до последней ноды).
    :return:
    """
    with open("last_state", "w") as state_file:
        state_file.write(STATE1)

    db = connect()
    nodes = True
    # мы будем исполнять эту часть программы, пока в базе данных остаются не проверенные ноды
    while nodes:
        cur = db.cursor()
        # выберем в случайном порядке ноды, в которых парсер еще не успел побывать
        cur.execute(
            """
SELECT 
    *
FROM 
    category
WHERE
    is_traversed = 0
ORDER BY 
    RANDOM();
                """
        )
        nodes = cur.fetchall()
        cur.close()
        logging.info(
            f"В базе найдено {len(nodes)} непроверенных категорий. "
            f"Это должно занять {str(datetime.timedelta(seconds=(STATE1_SLEEP_SECONDS * len(nodes))))} "
            "при условии непрерывного безошибочного парсинга."
        )

        # мы получили ноды, идем в них
        for node in nodes:
            driver.get(node["url"])
            # ждем
            sleep_about(STATE1_SLEEP_SECONDS)

            # здесь идет 3 возможных варианта развития событий:
            # 1. Например, как в "Ноутбуки": мы сразу переходим в список товаров.
            # 2. Например, как в "Винные шкафы": мы переходим в сетку товаров.
            # 3. Например, как в "Крупная бытовая техника": мы переходим в список подкатегорий.

            # предполагаем, что мы в варианте 3, пробуем найти контейнер подкатегорий:
            try:
                parent_element = WebDriverWait(driver, 10).until(
                    expected_conditions.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.CatalogCategoryCardWrapper__content-flex"))
                )
                # находим линки на подкатегории
                subcategory_links = parent_element.find_elements(By.CSS_SELECTOR, "a.CatalogCategoryCard__link")
                cur = db.cursor()
                cur.execute(
                    """
UPDATE 
    category
SET
    is_leaf      = 0, 
    is_traversed = 1
WHERE
    id = ?;
                    """,
                    (node["id"], )
                )
                db.commit()
                cur.close()

                for subcategory_link in subcategory_links:
                    url = subcategory_link.get_attribute("href")
                    name = subcategory_link.text
                    subcategory = {
                        "id": hashlib.sha256(url.encode("utf-8")).hexdigest(),
                        "url": url, "name": name, "parent_category_id": node["id"]
                    }
                    logging.info(
                        f"Подкатегория: name='{subcategory['name']}', url={subcategory['url']}, id={subcategory['id']}"
                    )
                    cur = db.cursor()
                    cur.execute(
                        """
INSERT INTO
    category (id, name, url, is_traversed, is_leaf, parent_category_id)
VALUES
    (:id, :name, :url, 1, 1, :parent_category_id)
ON CONFLICT
DO
    NOTHING
                        """,
                        subcategory
                    )
                    db.commit()
                    cur.close()
            except TimeoutException:
                # очевидно, раз мы не нашли элемент, то мы, должно быть, уже в каталоге.
                try:
                    # мы пробуем дождаться грида продуктов
                    item_grid = WebDriverWait(driver, 10).until(
                        expected_conditions.presence_of_element_located(
                            (By.CSS_SELECTOR, "div.ProductCardCategoryList__grid-container")
                        )
                    )
                    # выясняем, в каком виде представлены данные
                    data_view_type = item_grid.get_attribute("data-initial-view-type")
                    if data_view_type == "grid":
                        # если сетка
                        item_hrefs = item_grid.find_elements(By.CSS_SELECTOR, "a.ProductCardVertical__name")
                    elif data_view_type == "list":
                        # если список
                        item_hrefs = item_grid.find_elements(By.CSS_SELECTOR, "a.ProductCardHorizontal__title")
                    else:
                        # в другом случае неизвестно, что делать
                        logging.error(
                            f"Неизвестный вариант сетки товаров: '{data_view_type}'. "
                            f"Встретился на {node['url']}. Перейдите по ссылке, чтобы посмотреть, что происходит."
                        )
                        continue
                    # дальше идем по hrefам и собираем ссылки
                    for item_href in item_hrefs:
                        url = item_href.get_attribute("href")
                        item = {
                            "category_id": node["id"],
                            "id": hashlib.sha256(url.encode("utf-8")).hexdigest(),
                            "url": url
                        }
                        # записываем товар в базу
                        cur = db.cursor()
                        cur.execute(
                            """
INSERT INTO
    item (category_id, id, name, url, is_traversed, price)
VALUES
    (:category_id, :id, NULL, :url, 0, NULL)
ON CONFLICT
DO
    NOTHING
;
                            """,
                            item
                        )
                        db.commit()
                        cur.close()
                    # если все прошло успешно, помечаем, что эта нода - лист, и что она пройдена
                    cur = db.cursor()
                    cur.execute(
                        """
UPDATE 
    category
SET
    is_leaf      = 1, 
    is_traversed = 1
WHERE
    id = ?;
                        """,
                        (node["id"],)
                    )
                    db.commit()
                    cur.close()
                except TimeoutException:
                    filename = take_screenshot()
                    # если мы не дождались, то значит мы в неизвестном месте.
                    logging.error(
                        f"Неизвестное положение парсера на {node['url']}. "
                        "Ожидалось, что он находится в списке товаров или в списке подкатегорий. "
                        f"Состояние страницы запечатлено на скриншоте: {filename}"
                    )
                    # не заканчиваем парсинг, игнорируем ошибку, пробуем идти дальше
    db.close()


def state2():
    """
    Запускать нужно, чтобы взять все товары.
    :return:
    """
    n = 0

    with open("last_state", "w") as state_file:
        state_file.write(STATE2)

    db = connect()
    items = True
    # мы будем исполнять эту часть программы, пока в базе данных остаются непроверенные товары
    while items:
        cur = db.cursor()
        # выберем в случайном порядке товары, в которых парсер еще не успел побывать
        cur.execute(
            """
SELECT 
    *
FROM 
    item
WHERE
    is_traversed = 0
ORDER BY 
    RANDOM();
                """
        )
        items = cur.fetchall()
        cur.close()
        logging.info(
            f"В базе найдено {len(items)} непроверенных ссылок на товары. "
            f"Это должно занять {str(datetime.timedelta(seconds=(STATE2_SLEEP_SECONDS * len(items))))} "
            "при условии непрерывного безошибочного парсинга."
        )

        # и пойдем по товарам
        for item in items:
            record = {"id": item["id"]}
            driver.get(item["url"])
            # ждем
            sleep_about(STATE2_SLEEP_SECONDS)
            # подождем, пока появится цена
            try:
                product_card = WebDriverWait(driver, 10).until(
                    expected_conditions.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.ProductCardLayout")
                    )
                )
                title_h1 = product_card.find_element(By.CSS_SELECTOR, "h1.ProductHeader__title")

                title = title_h1.text
                record["name"] = title
                logging.info(f"Название: '{title}'")

                try:
                    price_span = product_card.find_element(By.CSS_SELECTOR,
                                                           "span.ProductHeader__price-default_current-price")
                    logging.info(f"Цена: '{price_span.text}'")
                    price = price_span.text.replace(" ", "")
                    try:
                        price = int(price)
                        record["price"] = price
                    except ValueError:
                        logging.error(
                            f"Парсинг цены: не удалось превратить '{price}' в целочисленное. "
                            f"Проверьте страницу: {item['url']}"
                        )
                        continue
                except NoSuchElementException as ex:
                    logging.warning(
                        f"Не найдено цены товара: {item['url']}. Ошибка: {ex}."
                    )

                image_tags = product_card.find_elements(By.CSS_SELECTOR, "img.PreviewList__image")
                for image_number, image_tag in zip(range(image_tags), image_tags):
                    image_url = image_tag.get_attribute("src")
                    image_record = {
                        "item_id": item["id"],
                        "url": image_url,
                        "image_number": image_number,
                    }
                    cur = db.cursor()
                    cur.execute(
                        """
INSERT INTO
    item_image (item_id, url, path, number)
VALUES
    (:item_id, :url, NULL, :image_number)
ON CONFLICT DO
    UPDATE
        SET 
            item_id = :item_id,
            number  = :image_number
;
                            """,
                        image_record
                    )
                    db.commit()
                    cur.close()
                try:
                    specs_div = driver.find_element(By.CSS_SELECTOR, "div.Specifications")
                    specs_rows = specs_div.find_elements(By.CSS_SELECTOR, "div.Specifications__row")
                    for specs_row in specs_rows:
                        key = specs_row.find_element(By.CSS_SELECTOR, "div.Specifications__column_name").text
                        value = specs_row.find_element(By.CSS_SELECTOR, "div.Specifications__column_value").text
                        cur = db.cursor()
                        cur.execute(
                            """
INSERT INTO
    item_property (item_id, name, value)
VALUES
    (:item_id, :key, :value)
;
                            """,
                            {"key": key, "value": value, "item_id": item["id"]}
                        )
                        db.commit()
                        cur.close()
                except NoSuchElementException as ex:
                    logging.warning(
                        f"Не найдено спецификации товара: {item['url']}. Ошибка: {ex}."
                    )

                cur = db.cursor()
                if "price" in record:
                    cur.execute(
                        """
UPDATE 
    item
SET
    name         = :name,
    price        = :price,
    is_traversed = 1
WHERE
    id = :id
                        """,
                        record
                    )
                else:
                    cur.execute(
                        """
UPDATE 
    item
SET
    name         = :name,
    is_traversed = 1
WHERE
    id = :id
                        """,
                        record
                    )
                db.commit()
                cur.close()

                n += 1
                logging.info(f"{n}/{len(items)}")
            except TimeoutException as ex:
                def error():
                    filename = take_screenshot()
                    logging.error(
                        f"Не найдено необходимого для парсинга элемента: {ex}. "
                        f"Ошибка произошла на странице: {item['url']}. "
                        f"Состояние страницы на момент ошибки: {filename}."
                    )

                try:
                    rate_limit_div = WebDriverWait(driver, 10).until(
                        expected_conditions.presence_of_element_located(
                            (By.CSS_SELECTOR, "div.request-limit-page")
                        )
                    )
                    if rate_limit_div:
                        logging.error(f"Попалась капча! Успел спарсить {n} объявлений до ее появления.")
                        raise ParserError("Введите капчу и запустите скрипт заново.")
                    else:
                        error()
                        continue
                except NoSuchElementException:
                    error()
                    continue
            except Exception as ex:
                logging.error(
                    f"Неизвестная ошибка во время парсинга товара: {type(ex)}, {ex}"
                )
                continue
    # закрываем базу данных
    db.close()


def cleanup():
    with open("last_state", "w") as state_file:
        state_file.write(CLEANUP)

    logging.info("СОСТОЯНИЕ: cleanup")
    db = connect()

    logging.info("Запрос в базу, ищем изображения без предмета.")
    cur = db.cursor()
    cur.execute(
        """
SELECT
    path
FROM 
    item_image
WHERE
    item_id IS NULL
;
        """
    )
    images = cur.fetchall()
    cur.close()

    logging.info(f"Найдено {len(images)} таких изображений.")
    logging.info("Удаляем их с диска.")
    # удаляем изображения, у которых нет "хозяина"
    for image in images:
        path_to_image = os.path.join(PATH_TO_IMAGES, image["path"])
        if os.path.exists(path_to_image):
            os.remove(path_to_image)
    logging.info("Удалено.")

    logging.info("Удаляем из базы.")
    # а также удаляем их из базы данных
    cur = db.cursor()
    cur.execute(
        """
DELETE
FROM
    item_image
WHERE
    item_id IS NULL
;
        """
    )
    db.commit()
    cur.close()
    logging.info("Удалено.")

    db.close()


def main():
    with open("last_state", "r") as state_file:
        state = state_file.read()
    if not state or state == STATE0:
        state0()
        state1()
        state2()
        cleanup()
    elif state == STATE1:
        state1()
        state2()
        cleanup()
    elif state == STATE2:
        state2()
        cleanup()
    elif state == CLEANUP:
        cleanup()
    else:
        logging.error(f"Неизвестное состояние: '{state}'.")


if __name__ == "__main__":
    main()
