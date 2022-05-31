from database import connect as sqlite_connect
from mysql.connector import connect as mysql_connect
import os


LANGUAGE_ID = 1
ATTRIBUTE_GROUP_ID = 7
STOCK_STATUS_ID = 7
MANUFACTURER_ID = 0
TAX_CLASS_ID = 0
WEIGHT_CLASS_ID = 1
LENGTH_CLASS_ID = 1
STORE_ID = 0
LAYOUT_ID = 0

PATH_TO_IMAGES = "catalog/products/"


sqlite_db = sqlite_connect()
mysql_db = mysql_connect(host="127.0.0.1", port=3306, database="admin_bd", user="admin_bd", password="QRKyJhiNP9")


def format_attribute_name(attribute_name):
    # while len(attribute_name) > 0 and not attribute_name[-1].isalnum():
    #     attribute_name = attribute_name[:-1]
    # while len(attribute_name) > 0 and not attribute_name[0].isalnum():
    #     attribute_name = attribute_name[1:]
    attribute_name = attribute_name.strip()
    if attribute_name[-1] == ":":
        attribute_name = attribute_name[:-1]
    return attribute_name.strip()


def format_description(desc):
    return f"&lt;p&gt;{desc}&lt;/p&gt;"


def get_root_categories():
    cur = sqlite_db.cursor()
    cur.execute(
        """
SELECT
    child.*
FROM
    category AS child
    LEFT JOIN
        category AS parent
    ON
        parent.id = child.parent_category_id
WHERE
    parent.id IS NULL;
        """
    )
    categories = cur.fetchall()
    cur.close()
    return categories


def get_attributes():
    cur = sqlite_db.cursor()
    cur.execute(
        """
SELECT
    *
FROM
    item_property
;
        """
    )
    attributes = cur.fetchall()
    cur.close()
    return attributes


def get_attributes_for_product(product_id):
    cur = sqlite_db.cursor()
    cur.execute(
        """
SELECT
    *
FROM
    item_property
WHERE
    item_id = ?
;
        """,
        (product_id, )
    )
    properties = cur.fetchall()
    cur.close()
    return properties


def get_images_for_product(product_id):
    cur = sqlite_db.cursor()
    cur.execute(
        """
SELECT
    *
FROM
    item_image
WHERE
    item_id = ? AND path IS NOT NULL
ORDER BY
    number ASC
;
        """,
        (product_id, )
    )
    images = cur.fetchall()
    cur.close()
    return images


def get_child_categories(category_id):
    cur = sqlite_db.cursor()
    cur.execute(
        """
SELECT 
    *
FROM
    category
WHERE
    parent_category_id = ?
;
        """,
        (category_id, )
    )
    categories = cur.fetchall()
    cur.close()
    return categories


def get_products():
    cur = sqlite_db.cursor()
    cur.execute(
        """
SELECT
    *
FROM
    item
;
        """

    )
    items = cur.fetchall()
    cur.close()
    return items


def generate_oc_category_path(ids):
    rows = []
    for i in range(len(ids)):
        for j in range(0, i + 1):
            rows.append(
                (ids[i], ids[j], j)
            )
    return rows


def main():
    category_id_map = dict()
    category_tree = dict()

    root_categories = get_root_categories()
    root_category_ids = []
    parent_category_ids = []

    for category in root_categories:
        parent_category_ids.append(category["id"])

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_category(
        `parent_id`, 
        `top`, 
        `column`, 
        `sort_order`, 
        `status`, 
        `date_added`, 
        `date_modified`
    )
VALUES
    (0, 0, 1, 0, 1, NOW(), NOW())
;
                """
            )
            mysql_db.commit()

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
SELECT LAST_INSERT_ID();
                """
            )
            category_id = cursor.fetchone()[0]

        category_id_map[category['id']] = category_id
        category_tree[category_id] = []
        root_category_ids.append(category_id)

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_category_description (
        `category_id`,
        `language_id`,
        `name`,
        `description`,
        `meta_title`,
        `meta_description`,
        `meta_keyword`
    )
VALUES
    (%s, %s, %s, '', %s, '', '')
;
                """,
                (category_id, LANGUAGE_ID, category["name"], category["name"])
            )
            mysql_db.commit()

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_category_to_layout (
        `category_id`,
        `store_id`,
        `layout_id`
    )
VALUES
    (%s, 0, 0)
;
                """, (category_id, )
            )
            mysql_db.commit()

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_category_to_store (
        `category_id`,
        `store_id`
    )
VALUES
    (%s, 0)
;         
                """, (category_id, )
            )
            mysql_db.commit()

    while parent_category_ids:
        _parent_category_ids = parent_category_ids
        parent_category_ids = []

        for parent_category_id in _parent_category_ids:
            child_categories = get_child_categories(parent_category_id)

            for category in child_categories:
                # родительские категории следующего уровня
                parent_category_ids.append(category["id"])

                with mysql_db.cursor() as cursor:
                    cursor.execute(
                        """
INSERT INTO
    oc_category(
        `parent_id`, 
        `top`, 
        `column`, 
        `sort_order`, 
        `status`, 
        `date_added`, 
        `date_modified`
    )
VALUES
    (%s, 0, 1, 0, 1, NOW(), NOW())
;
                        """,
                        (parent_category_id, )
                    )
                    mysql_db.commit()

                with mysql_db.cursor() as cursor:
                    cursor.execute(
                        """
SELECT LAST_INSERT_ID();
                        """
                    )
                    category_id = cursor.fetchone()[0]

                category_id_map[category['id']] = category_id
                # также запишем в дерево
                category_tree[category_id] = []
                # запишем, что это дочка
                category_tree[category_id_map[parent_category_id]].append(category_id)

                with mysql_db.cursor() as cursor:
                    cursor.execute(
                        """
INSERT INTO
    oc_category_description (
        `category_id`,
        `language_id`,
        `name`,
        `description`,
        `meta_title`,
        `meta_description`,
        `meta_keyword`
    )
VALUES
    (%s, %s, %s, '', %s, '', '')
;
                        """,
                        (category_id, LANGUAGE_ID, category["name"], category["name"])
                    )
                    mysql_db.commit()

                with mysql_db.cursor() as cursor:
                    cursor.execute(
                        """
INSERT INTO
    oc_category_to_layout (
        `category_id`,
        `store_id`,
        `layout_id`
    )
VALUES
    (%s, 0, 0)
;
                        """,
                        (category_id,)
                    )
                    mysql_db.commit()

                with mysql_db.cursor() as cursor:
                    cursor.execute(
                        """
INSERT INTO
    oc_category_to_store (
        `category_id`,
        `store_id`
    )
VALUES
    (%s, 0)
;         
                        """,
                        (category_id,)
                    )
                    mysql_db.commit()

    # теперь нужно создать "граф"
    graph = []
    arrays = [[id_] for id_ in root_category_ids]

    # не будет цикла в графе - этот алгоритм закончится
    while arrays:
        for _ in range(len(arrays)):
            arr = arrays.pop()
            id_ = arr[-1]
            child_ids = category_tree[id_]
            # если нет детей, то это лист
            if not child_ids:
                graph.append(arr)
            # в другом случае добавляем всех детей и путь до них
            else:
                for child_id in child_ids:
                    new_arr = arr + [child_id]
                    arrays.append(new_arr)

    # сюда сохраняем предыдущие ноды для последней
    leaf_map = dict()
    # теперь, когда есть путь от каждого начала к каждому концу, мы эти пути превращаем в oc_category_path
    oc_category_paths = set()
    for path in graph:
        leaf_id = path[-1]
        # все вплоть до листа
        leaf_map[leaf_id] = [*path[:-1]]
        category_path = generate_oc_category_path(path)
        # одним махом избавляемся от "повторок" и складываем все данные в одну коллекцию
        oc_category_paths = oc_category_paths.union(category_path)

    # загружаем "пути" в базу данных
    for oc_category_path in oc_category_paths:
        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_category_path (
        `category_id`,
        `path_id`,
        `level`
    )
VALUES
    (%s, %s, %s)
;
                """,
                oc_category_path
            )
            mysql_db.commit()

    # с категориями покончено, теперь к товарам
    # перво-наперво, нужно перенести "атрибуты" и избавиться от повторяющихся ключей (сплющить данные)
    unique_attributes = set()
    attribute_name_to_id = dict()
    all_attributes = get_attributes()
    # находим уникальные атрибуты
    for attribute in all_attributes:
        attribute_name = format_attribute_name(attribute["name"])
        unique_attributes.add(attribute_name)

    # для каждого из них создаем атрибут + описание (название)
    for attribute_name in unique_attributes:
        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_attribute (
        `attribute_group_id`,
        `sort_order`
    )
VALUES
    (%s, 0)
;
                """,
                (ATTRIBUTE_GROUP_ID, )
            )
            mysql_db.commit()

        with mysql_db.cursor() as cursor:
            cursor.execute(
                    """
SELECT LAST_INSERT_ID();
                    """
            )
            attribute_id = cursor.fetchone()[0]

        attribute_name_to_id[attribute_name] = attribute_id

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_attribute_description (
        `attribute_id`,
        `language_id`,
        `name`
    )
VALUES
    (%s, %s, %s)
;
                """,
                (attribute_id, LANGUAGE_ID, attribute_name)
            )
            mysql_db.commit()

    # шаблоны атрибутов добавлены, теперь добавляем товары и атрибуты к ним
    products = get_products()

    # маппинг из родного айди в опенкарт
    product_id_map = dict()
    for product in products:
        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_product (
        `model`,
        `sku`,
        `upc`,
        `ean`,
        `jan`,
        `isbn`,
        `mpn`,
        `location`,
        `quantity`,
        `stock_status_id`,
        `image`,
        `manufacturer_id`,
        `shipping`,
        `price`,
        `points`,
        `tax_class_id`,
        `date_available`,
        `weight`,
        `weight_class_id`,
        `length`,
        `width`,
        `height`,
        `length_class_id`,
        `subtract`,
        `minimum`,
        `sort_order`,
        `status`,
        `viewed`,
        `date_added`,
        `date_modified`
    )
VALUES (
    %s,
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    1,
    %s,
    NULL,
    %s,
    1,
    %s,
    0,
    %s,
    NOW(),
    0.000,
    %s,
    0.000,
    0.000,
    0.000,
    %s,
    1,
    1,
    0,
    1,
    0,
    NOW(),
    NOW()
)
;
                """,
                (
                    # ЗДЕСЬ МАКСИМАЛЬНО 64 СИМВОЛА!!!
                    product["name"][:64],
                    STOCK_STATUS_ID,
                    MANUFACTURER_ID,
                    product["price"] if product["price"] else 0,
                    TAX_CLASS_ID,
                    WEIGHT_CLASS_ID,
                    LENGTH_CLASS_ID,
                )
            )
            mysql_db.commit()

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
SELECT LAST_INSERT_ID();
                """
            )
            product_id = cursor.fetchone()[0]

        product_id_map[product["id"]] = product_id
        # теперь добавляем описание
        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_product_description (
        `product_id`,
        `language_id`,
        `name`,
        `description`,
        `tag`,
        `meta_title`,
        `meta_description`,
        `meta_keyword`
    )
VALUES (
    %s,
    %s,
    %s,
    '',
    '',
    %s,
    '',
    ''
)
;
                """,
                (product_id, LANGUAGE_ID,
                 # ЗДЕСЬ МАКСИМАЛЬНО 255 СИМВОЛОВ
                 product["name"][:255], product["name"][:255])
            )
            mysql_db.commit()

        # отсюда получаем айди категории внутри опенкарта
        product_category_id = category_id_map[product["category_id"]]
        # отсюда получаем все категории, внутри которых находится товар
        product_category_id_hierarchy = leaf_map[product_category_id] + [product_category_id]
        # теперь добавляем товар в категории
        for category_id in product_category_id_hierarchy:
            with mysql_db.cursor() as cursor:
                cursor.execute(
                    """
INSERT INTO
    oc_product_to_category (
        `product_id`,
        `category_id`
    )    
VALUES (
    %s,
    %s
)
;
                    """,
                    (product_id, category_id)
                )
                mysql_db.commit()
            # теперь товар высвечивается во всех категориях,
            # которые являются родительскими по отношению к последней в иерархии
        # теперь стандартные значения в oc_product_to_store и oc_product_to_layout
        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO
    oc_product_to_store (
        `product_id`,
        `store_id`   
    )
VALUES (
    %s,
    %s
)
;
                """,
                (product_id, STORE_ID)
            )
            mysql_db.commit()

        with mysql_db.cursor() as cursor:
            cursor.execute(
                """
INSERT INTO 
    oc_product_to_layout (
        `product_id`,
        `store_id`,
        `layout_id`
    )
VALUES (
    %s,
    %s,
    %s
)
;
                """,
                (product_id, STORE_ID, LAYOUT_ID)
            )
            mysql_db.commit()

        # теперь для товара получаем атрибуты
        attributes = get_attributes_for_product(product["id"])
        # бывает так, что один и тот же атрибут повторяется
        # например, '' - какой-то умник из ситилика подумал, что охуенно будет сделать пустую строку атрибутом
        # мы их "сплющим", то есть вместо нескольких '' будет один атрибут, так как опенкарт не позволяет повторять
        # атрибуты для одного и того же товара
        flat_attributes = dict()
        for attribute in attributes:
            attribute_name = format_attribute_name(attribute["name"])
            attribute_oc_id = attribute_name_to_id[attribute_name]
            if attribute_oc_id in flat_attributes:
                flat_attributes[attribute_oc_id].append(attribute["value"])
            else:
                flat_attributes[attribute_oc_id] = [attribute["value"]]

        attributes = []
        # мы их сплющиваем:
        # из [('', 'a'), ('', 'b'), ('', 'c'), ('d', 'd')]
        # в  [('', 'a; b; c'), ('d', 'd')]
        for attribute_oc_id in flat_attributes.keys():
            attributes.append(
                {"id": attribute_oc_id, "value": '; '.join(flat_attributes[attribute_oc_id])}
            )

        # теперь по очереди их вставляем
        for attribute in attributes:
            with mysql_db.cursor() as cursor:
                cursor.execute(
                    """
INSERT INTO
    oc_product_attribute (
        `product_id`,
        `attribute_id`,
        `language_id`,
        `text`
    )
VALUES (
    %s,
    %s,
    %s,
    %s
)
;
                    """,
                    (product_id, attribute["id"], LANGUAGE_ID, attribute["value"])
                )
                mysql_db.commit()

        # последнее: нужно вставить картинки
        images = get_images_for_product(product["id"])
        # если изображения есть в базе
        if len(images) > 0:
            # первое изображение установим, как заглавное
            with mysql_db.cursor() as cursor:
                cursor.execute(
                    """
UPDATE
    `oc_product`
SET
    `image` = %s
WHERE
    `product_id` = %s
;
                    """,
                    (os.path.join(PATH_TO_IMAGES, images[0]["path"]), product_id)
                )
                sqlite_db.commit()
            # остальные просто добавим к карточке товара
            for image in images[1:]:
                with mysql_db.cursor() as cursor:
                    cursor.execute(
                        """
INSERT INTO
    oc_product_image (
        `product_id`,
        `image`,
        `sort_order`
    )
VALUES (
    %s,
    %s,
    %s
)
;
                        """,
                        (product_id, os.path.join(PATH_TO_IMAGES, image["path"]),
                         image["number"] if image["number"] else 0)
                    )
                    mysql_db.commit()
        # изображения добавлены
# вроде как все


if __name__ == "__main__":
    main()
