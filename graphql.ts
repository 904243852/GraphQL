import { DB } from "https://deno.land/x/sqlite@v3.3.0/mod.ts";

type Record = {
    [name: string]: any;
}

type SchemaTable = {
    table: string;
    joined: {
        column: string;
        parent: string;
        isCollection?: false;
    };
    properties: SubSchema;
}
type SchemaColumn = {
    column: string;
    isPrimaryKey?: true;
}
type SubSchema = {
    [name: string]: SchemaTable | SchemaColumn;
}
export type Schema = {
    [name: string]: Pick<SchemaTable, "table" | "properties">;
}

type QueryDSLTableCondition = {
    field: string;
    operator: "in" | "eq";
    value: object[] | object;
}
type QueryDSLTable = {
    properties: QueryDSL | null;
    conditions?: QueryDSLTableCondition[];
    options?: {
        skip?: number;
        limit?: number;
    };
}
export type QueryDSL = {
    [name: string]: QueryDSLTable | Record | null;
}
type ExtendedQueryDSL = {
    [name: string]: {
        foreignKeys?: string[];
        conditions?: {
            field: string;
            operator: "in";
            value: object[];
        }[];
    }
}

export type MutationDSL = {
    [name: string]: object | MutationDSL;
}

export abstract class GraphQL {
    private schema: SubSchema;

    constructor(schema: Schema) {
        this.schema = schema as SubSchema;
    }

    public query(dsl: QueryDSL, schema = this.schema, exdsl?: ExtendedQueryDSL) {
        return Object.keys(dsl).reduce((result: Record, name: string) => {
            let tdsl = dsl[name] as QueryDSLTable;

            // 若 dsl 没有 properties 属性，则将 dsl 作为 record 查询
            if (tdsl && !("properties" in tdsl)) {
                let properties = tdsl as Record,
                    conditions = Object.keys(properties).reduce((p, c) => {
                        properties[c] !== null && p.push({ field: c, operator: "eq", value: properties[c] }); // 如果 record 的属性值不为 null，则将该属性值作为查询条件
                        return p;
                    }, [] as QueryDSLTableCondition[]);
                tdsl = { properties, conditions };
            }

            // 查询表模型
            let model = <SchemaTable>schema[name];
            if (!model) {
                throw new Error(`Can not find the model ${name} in schema.`);
            }
            if (!model.table) {
                throw new Error(`The ${name} is not a table schema.`);
            }

            // 初始化请求
            let request = {
                properties: tdsl?.properties || <QueryDSL>model.properties, // 如果请求的 properties 为 null，表示默认查询所有字段
                options: {
                    skip: 0,
                    limit: 5000,
                    ...tdsl?.options
                },
                conditions: (tdsl?.conditions || []).map(({ field, operator, value }) => { return { field: (model.properties[field] as SchemaColumn).column, operator, value }; }).concat(exdsl?.[name]?.conditions || []),
                foreignKeys: exdsl?.[name]?.foreignKeys || []
            };
            let subrequests: QueryDSL = {};

            // 构建查询列集合
            let columns = {} as { [name: string]: { alias?: string; display?: boolean; isForeignKey?: true; } };
            for (let n of request.foreignKeys) { // 处理外键所在的列
                columns[n] = { display: false, isForeignKey: true };
            }
            for (let n in request.properties) { // 处理属性所在的列
                if (!model.properties[n]) {
                    throw new Error(`Can not find the property ${n} in schema.`);
                }
                if ((model.properties[n] as SchemaTable).table) { // 如果当前属性是子模型
                    let p = (model.properties[n] as SchemaTable).joined.parent;
                    columns[p] = columns[p] || { display: false };
                    subrequests[n] = request.properties[n];
                } else { // 否则当前属性是列
                    let c = (model.properties[n] as SchemaColumn).column;
                    columns[c] = { ...columns[c], alias: n, display: true };
                }
            }

            // 构建查询条件集合
            let conditions: string[] = [],
                params: object[] = [];
            for (let condition of request.conditions) {
                if (condition.value == null) {
                    throw new Error(`The value of condition ${condition.field} is null.`);
                }
                if (condition.operator === "in") {
                    let values = (<object[]>condition.value).filter(v => v != null); // 去除 null 值
                    conditions.push(`${condition.field} ${condition.operator} (${values.map(v => "?").join(", ")})`);
                    params.push(...values);
                }
                if (condition.operator === "eq") {
                    conditions.push(`${condition.field} = ?`);
                    params.push(condition.value);
                }
            }

            let records = [];

            // 查询
            let datasets = this.onSelect(`select ${Object.keys(columns).join(", ")} from ${model.table} where ${["1 = 1"].concat(conditions).join(" and ")} limit ${request.options.skip},${request.options.limit}`, { params }) as Record[];
            if (datasets.length > 0) {
                // 遍历子请求，构造外键和外键关联条件
                let subexrequests: ExtendedQueryDSL = {};
                for (let n in subrequests) {
                    let { column: c, parent: p } = (model.properties[n] as SchemaTable).joined;
                    subexrequests[n] = {
                        foreignKeys: [c],
                        conditions: [{
                            field: c,
                            operator: "in",
                            value: this.uniq(datasets.map(d => d[p]))
                        }]
                    };
                }

                // 如果有子属性请求，查询子对象
                let subdatasets = this.query(subrequests, model.properties, subexrequests) as { [name: string]: { data: object, keys?: Record; }[] };

                // 先遍历所有子对象，按照外键分类，便于后续回写子对象值
                let subdatasetsmap: { [name: string]: { [key: string]: object[] } } = {};
                for (let n in subdatasets) {
                    let { column: c } = (model.properties[n] as SchemaTable).joined;
                    subdatasetsmap[n] = {};
                    for (let subdataset of subdatasets[n]) {
                        if (!subdataset.keys?.[c]) {
                            throw new Error(`Can not find the property ${c} in sub hide dataset.`);
                        }
                        subdatasetsmap[n][subdataset.keys[c]] = subdatasetsmap[n][subdataset.keys[c]] || [];
                        subdatasetsmap[n][subdataset.keys[c]].push(subdataset.data);
                    }
                }

                // 遍历对象，回写属性值
                for (let dataset of datasets) {
                    let data = {} as Record, // 数据（需要随最终结果返回）
                        keys = {} as Record; // 键值（不需要返回，用于外键关联）

                    for (let n in columns) {
                        if (columns[n].display) { // 如果需要作为结果返回，属性名称需要重命名
                            data[columns[n].alias || n] = <object>dataset[n];
                        }
                        if (columns[n].isForeignKey) { // 如果是外键，则将属性放入不可见属性集中
                            keys[n] = <object>dataset[n];
                        }
                    }

                    for (let n in subdatasetsmap) {
                        let submodel = model.properties[n] as SchemaTable;

                        // 根据外键过滤子对象集合，并赋值给父对象
                        dataset[n] = subdatasetsmap[n][dataset[submodel.joined.parent]];

                        if (submodel.joined.isCollection === false) { // 如果子模型非集合，则返回第一个子对象
                            dataset[n] = (<object[]>dataset[n])?.pop();
                        }

                        data[n] = dataset[n]; // 子模型需要作为结果返回
                    }

                    records.push({ data, keys });
                }
            }

            result[name] = schema == this.schema
                ? records.map(r => r.data) // 隐藏根元素的 keys 记录
                : records;
            return result;
        }, {});
    }

    public mutate(dsl: MutationDSL, schema = this.schema): any {
        const request2dataset = function (name: string, record: { data: Record | Record[]; keys?: Record; }, schema: SubSchema): { dataset: object; request: object; }[] { // 请求转数据库记录
            if (!record.data) {
                throw new Error("The request data can not be null.");
            }

            let model = schema[name] as SchemaTable;

            // 解析请求中的键值属性
            let keys = Object.keys(record.keys || {}).reduce((r: Record, n: string) => {
                r[n] = record?.keys?.[n];
                return r;
            }, {});

            return (record.data instanceof Array ? <Record[]>record.data : [record.data]).map(request => {
                let dataset = Object.keys(request).reduce((r: Record, n: string) => { // 解析请求中的属性
                    if (!model.properties?.[n]) {
                        throw new Error(`Can not find the property ${n} in schema ${name}.`);
                    }
                    if ((model.properties[n] as SchemaColumn).column) {
                        r[(model.properties[n] as SchemaColumn).column] = <object>request[n];
                    }
                    return r;
                }, {});
                return {
                    dataset: {
                        ...keys,
                        ...dataset
                    },
                    request
                };
            });
        };

        for (let name in dsl) {
            // 查询表模型
            let model = schema[name] as SchemaTable;
            if (!model) {
                throw new Error(`Can not find the model ${name} in schema.`);
            }

            // 校验请求
            if (!dsl[name]) {
                throw new Error(`The request ${name} can not be null.`);
            }
            if (typeof (dsl[name]) === "string") {
                throw new Error(`The request ${name} is invalid.`);
            }

            let requests = dsl[name] instanceof Array ? <object[]>dsl[name] : [dsl[name]],
                request2datasets = [] as { dataset: Record; request: Record; }[];
            for (let request of requests) {
                if (schema == this.schema) { // 第一次递归需要将根元素包装下
                    request2datasets = request2datasets.concat(request2dataset(name, { data: request }, schema)); // 构建数据库请求记录集合
                } else { // 否则已嵌套 data 和 keys
                    request2datasets = request2datasets.concat(request2dataset(name, <{ data: object; keys: object; }>request, schema));
                }
            }

            // 构建（前置）子请求
            let subrequests0 = {} as { [name: string]: { data: object; keys: object; }[]; };
            for (let request2dataset of request2datasets) {
                Object.keys(request2dataset.request)
                    .filter(n => (model.properties[n] as SchemaTable).table) // 筛选出子对象名称
                    .forEach(n => {
                        let submodel = (model.properties[n] as SchemaTable);

                        // 如果子模型主键关联父模型，则需要先保存子模型以生成子对象主键
                        if ((submodel.properties[Object.keys(submodel.properties).filter(sn => (submodel.properties[sn] as SchemaColumn).column === submodel.joined.column).pop() || ""] as SchemaColumn)?.isPrimaryKey !== true) {
                            return;
                        }
                        if (submodel.joined.isCollection !== false) {
                            throw new Error(`The sub model ${n} should not be a collection for the foreign key ${submodel.joined.parent} is a primary key of sub model.`);
                        }
                        subrequests0[n] = subrequests0[n] || [];
                        subrequests0[n].push({
                            data: <object>request2dataset.request[n],
                            keys: []
                        });
                    });
            }
            // 保存（前置）子对象
            if (Object.keys(subrequests0).length) {
                let subresults = this.mutate(subrequests0, model.properties) as { [name: string]: { data: Record; keys: Record; }[]; };

                Object.keys(subrequests0)
                    .forEach(n => {
                        let submodel = model.properties[n] as SchemaTable,
                            primaryKey = Object.keys(submodel.properties).filter(i => (submodel.properties[i] as SchemaColumn).column === submodel.joined.column).pop() || "";

                        // 遍历对象，将子对象以及子对象主键值回写至对象中
                        for (let i = 0; i < request2datasets.length; i++) {
                            let subresult = subresults[n][i];
                            request2datasets[i].request[n] = subresult!.data;
                            request2datasets[i].dataset[submodel.joined.parent] = subresult!.data[primaryKey];
                        }
                    });
            }

            // 保存并回写主键
            if (request2datasets.length > 0) {
                // 查询当前模型的主键
                let primaryKey = Object.keys(model.properties).filter(n => (model.properties[n] as SchemaColumn).isPrimaryKey === true).pop();
                if (!primaryKey) {
                    throw new Error(`Can not find the primary key in schema: ${name}.`);
                }

                let primaryKeyColumn = model.properties[primaryKey] as SchemaColumn;

                // 分类为需要新增的数据集和需要更新的数据集
                let data2insert: { dataset: Record; request: Record; }[] = [],
                    data2update: { dataset: Record; request: Record; }[] = [];
                for (let request2dataset of request2datasets) {
                    if (request2dataset.dataset[primaryKeyColumn.column]) {
                        data2update.push(request2dataset);
                    } else {
                        data2insert.push(request2dataset);
                    }
                }

                if (data2insert.length) {
                    let ids = this.onInsert(model.table, data2insert.map(d => d.dataset));
                    for (let i = 0; i < ids.length; i++) {
                        data2insert[i].request[primaryKey] = data2insert[i].dataset[primaryKeyColumn.column] = ids[i]; // 回写 id 至原始请求和数据库记录中
                    }
                }
                if (data2update.length) {
                    this.onUpdate(model.table, data2update.map(d => d.dataset), primaryKeyColumn.column);
                }
            }

            // 构建（后置）子请求
            let subrequests1 = {} as (typeof subrequests0);
            for (let request2dataset of request2datasets) {
                Object.keys(request2dataset.request)
                    .filter(n => !subrequests0[n])
                    .filter(n => (model.properties[n] as SchemaTable).table) // 筛选出子对象名称
                    .forEach(n => {
                        let submodel = (model.properties[n] as SchemaTable);
                        if (!request2dataset.dataset[submodel.joined.parent]) {
                            throw new Error(`The value of ${submodel.joined.parent} is required.`);
                        }
                        subrequests1[n] = subrequests1[n] || [];
                        subrequests1[n].push({
                            data: <object>request2dataset.request[n],
                            keys: {
                                [submodel.joined.column]: <object>request2dataset.dataset[submodel.joined.parent] // 将外键塞入子对象中
                            }
                        });
                    });
            }
            // 保存（后置）子对象
            if (Object.keys(subrequests1).length) {
                let subresults = this.mutate(subrequests1, model.properties) as { [name: string]: { data: object; keys: Record; }[]; };

                // 遍历父对象，将子对象回写至父对象中
                for (let request2dataset of request2datasets) {
                    Object.keys(subrequests1)
                        .forEach(n => {
                            let submodel = model.properties[n] as SchemaTable;
                            request2dataset.request[n] = ([] as Record[]).concat.apply([], subresults[n].filter(r => r.keys[submodel.joined.column] === request2dataset.dataset[submodel.joined.parent]).map(r => r.data));
                            if (submodel.joined.isCollection === false) {
                                request2dataset.request[n] = request2dataset.request[n].pop();
                            }
                        });
                }
            }

            if (schema == this.schema) {
                // 隐藏根元素的 keys 数据
                dsl[name] = dsl[name] instanceof Array
                    ? request2datasets.map(r => r.request) as object[]
                    : request2datasets.map(r => r.request).pop() as object;
            }
        }

        return dsl;
    }

    private uniq<T>(arr: T[]): T[] {
        return [...new Set(arr)];
    }

    protected abstract onSelect(stmt: string, options: { params: object[]; }): object[];

    protected abstract onInsert(table: string, datasets: object[]): string[];

    protected abstract onUpdate(table: string, datasets: object[], primaryKey: string): void;
}

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

//#region test

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

//#endregion
