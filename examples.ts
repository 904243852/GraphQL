import { GraphQL, Schema, Record } from "./graphql.ts";

import { DB } from "https://deno.land/x/sqlite@v3.3.0/mod.ts";

export class SqliteGraphQL extends GraphQL {
    private db: DB;

    constructor(schema: Schema, dbname: string, initsql: string) {
        super(schema);
        this.db = new DB(dbname);
        this.db.execute(initsql);
    }

    protected onSelect(stmt: string, options: { params: any[]; }): object[] {
        console.debug("\x1b[2m", "Query with", stmt, options.params, "\x1b[0m")
        return this.db.queryEntries<any>(stmt, options.params);
    }

    protected onInsert(table: string, datasets: Record[]): string[] {
        console.debug("\x1b[2m", "Insert with", datasets, "\x1b[0m")
        let that = this;
        return datasets.map(d => {
            that.db.query(`insert into ${table} (${Object.keys(d).join(", ")}) values (${Object.keys(d).map(n => "?").join(", ")})`, Object.keys(d).map(i => d[i]));
            return "" + that.db.lastInsertRowId;
        });
    }

    protected onUpdate(table: string, datasets: Record[], primaryKey: string): void {
        console.debug("\x1b[2m", "Update with", datasets, "\x1b[0m")
        datasets.forEach(d => {
            this.db.query(`update ${table} set ${Object.keys(d).filter(n => n !== primaryKey).map(n => `${n} = ?`).join(", ")} where ${primaryKey} = ?`, Object.keys(d).filter(n => n !== primaryKey).map(i => d[i]).concat([d[primaryKey]]));
        });
    }
}

// 创建一个基于 SQLite 的 GraphQL 实例
let graphql = new SqliteGraphQL({
    offering: {
        table: "Offering",
        properties: {
            spu: {
                column: "Id",
                isPrimaryKey: true
            },
            name: {
                table: "I18n",
                properties: {
                    id: {
                        column: "Id",
                        isPrimaryKey: true
                    },
                    zh: {
                        column: "Zh"
                    },
                    en: {
                        column: "En"
                    }
                },
                joined: {
                    column: "Id",
                    parent: "Name",
                    isCollection: false
                }
            },
            description: {
                column: "Description"
            },
            product: {
                table: "Product",
                properties: {
                    sku: {
                        column: "Id",
                        isPrimaryKey: true
                    },
                    price: {
                        column: "Price"
                    },
                    stock: {
                        column: "Stock"
                    },
                    attribute: {
                        table: "ProductAttribute",
                        properties: {
                            id: {
                                column: "Id",
                                isPrimaryKey: true
                            },
                            code: {
                                column: "Code"
                            },
                            value: {
                                column: "Value"
                            },
                            type: {
                                column: "Type"
                            }
                        },
                        joined: {
                            column: "ProductId",
                            parent: "Id"
                        }
                    }
                },
                joined: {
                    column: "OfferingId",
                    parent: "Id"
                }
            }
        }
    }
}, "my.db", `
    create table if not exists Offering (
        Id integer primary key autoincrement,
        Name text,
        Description text
    );
    create table if not exists Product (
        Id integer primary key autoincrement,
        Price text,
        Stock text,
        OfferingId text
    );
    create table if not exists ProductAttribute (
        Id integer primary key autoincrement,
        Code text,
        Value text,
        Type text,
        ProductId text
    );
    create table if not exists I18n (
        Id integer primary key autoincrement,
        Zh text,
        En text
    );
`);

console.info("Mutate:", JSON.stringify(
    // 保存记录
    graphql.mutate({
        offering: {
            name: {
                zh: "球",
                en: "ball"
            },
            description: "this is a ball",
            product: [{
                price: 2,
                attribute: [{
                    code: "color",
                    value: "red"
                }]
            }, {
                price: 2.5,
                attribute: [{
                    code: "color",
                    value: "green"
                }]
            }]
        }
    }), null, "    "
));

console.info("Query:", JSON.stringify(
    // 查询记录
    graphql.query({
        offering: {
            properties: null,
            conditions: [{ // 查询条件
                field: "spu",
                operator: "eq",
                value: 1
            }],
            options: { // 分页查询
                limit: 1,
                skip: 0
            }
        }
    }), null, "    "
));
console.info("Query:", JSON.stringify(
    // 查询记录
    graphql.query({
        offering: {
            name: null,
            description: "this is a ball", // 如果属性不为 null，则表示该属性作为查询条件
            product: null
        }
    }), null, "    "
));
