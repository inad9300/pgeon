import * as fs from 'fs'
import * as path from 'path'
import * as pg from 'pg'
import * as ts from 'typescript'
import {execSync} from 'child_process'

// Source: https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/core/Oid.java.
enum PgTypeId {
    BIT = 1560,
    BIT_ARRAY = 1561,
    BOOL = 16,
    BOOL_ARRAY = 1000,
    BOX = 603,
    BPCHAR = 1042,
    BPCHAR_ARRAY = 1014,
    BYTEA = 17,
    BYTEA_ARRAY = 1001,
    CHAR = 18,
    CHAR_ARRAY = 1002,
    DATE = 1082,
    DATE_ARRAY = 1182,
    FLOAT4 = 700,
    FLOAT4_ARRAY = 1021,
    FLOAT8 = 701,
    FLOAT8_ARRAY = 1022,
    INT2 = 21,
    INT2_ARRAY = 1005,
    INT4 = 23,
    INT4_ARRAY = 1007,
    INT8 = 20,
    INT8_ARRAY = 1016,
    INTERVAL = 1186,
    INTERVAL_ARRAY = 1187,
    JSON = 114,
    JSON_ARRAY = 199,
    JSONB_ARRAY = 3807,
    MONEY = 790,
    MONEY_ARRAY = 791,
    NAME = 19,
    NAME_ARRAY = 1003,
    NUMERIC = 1700,
    NUMERIC_ARRAY = 1231,
    OID = 26,
    OID_ARRAY = 1028,
    POINT = 600,
    POINT_ARRAY = 1017,
    REF_CURSOR = 1790,
    REF_CURSOR_ARRAY = 2201,
    TEXT = 25,
    TEXT_ARRAY = 1009,
    TIME = 1083,
    TIME_ARRAY = 1183,
    TIMESTAMP = 1114,
    TIMESTAMP_ARRAY = 1115,
    TIMESTAMPTZ = 1184,
    TIMESTAMPTZ_ARRAY = 1185,
    TIMETZ = 1266,
    TIMETZ_ARRAY = 1270,
    UNSPECIFIED = 0,
    UUID = 2950,
    UUID_ARRAY = 2951,
    VARBIT = 1562,
    VARBIT_ARRAY = 1563,
    VARCHAR = 1043,
    VARCHAR_ARRAY = 1015,
    VOID = 2278,
    XML = 142,
    XML_ARRAY = 143
}

const pgToJsType = {
    [PgTypeId.BOOL]: ['boolean', 'Boolean'],
    [PgTypeId.INT2]: ['number', 'Number', 'BigInt'],
    [PgTypeId.INT4]: ['number', 'Number', 'BigInt'],
    [PgTypeId.INT8]: ['number', 'Number', 'BigInt'],
    [PgTypeId.FLOAT4]: ['number', 'Number', 'BigInt'],
    [PgTypeId.FLOAT8]: ['number', 'Number', 'BigInt'],
    [PgTypeId.NUMERIC]: ['number', 'Number', 'BigInt'],
    [PgTypeId.TEXT]: ['string', 'String'],
    [PgTypeId.CHAR]: ['string', 'String'],
    [PgTypeId.BPCHAR]: ['string', 'String'],
    [PgTypeId.VARCHAR]: ['string', 'String'],
    [PgTypeId.DATE]: ['Date'],
    [PgTypeId.TIMESTAMP]: ['Date'],
    [PgTypeId.TIMESTAMPTZ]: ['Date'],
    [PgTypeId.BYTEA]: ['Uint8Array']
}

const jsToPgType = {
    'boolean': [PgTypeId.BOOL],
    'Boolean': [PgTypeId.BOOL],
    'number': [PgTypeId.INT2, PgTypeId.INT4, PgTypeId.INT8, PgTypeId.FLOAT4, PgTypeId.FLOAT8, PgTypeId.NUMERIC],
    'Number': [PgTypeId.INT2, PgTypeId.INT4, PgTypeId.INT8, PgTypeId.FLOAT4, PgTypeId.FLOAT8, PgTypeId.NUMERIC],
    'BigInt': [PgTypeId.INT2, PgTypeId.INT4, PgTypeId.INT8, PgTypeId.FLOAT4, PgTypeId.FLOAT8, PgTypeId.NUMERIC],
    'string': [PgTypeId.TEXT, PgTypeId.CHAR, PgTypeId.BPCHAR, PgTypeId.VARCHAR],
    'String': [PgTypeId.TEXT, PgTypeId.CHAR, PgTypeId.BPCHAR, PgTypeId.VARCHAR],
    'Date': [PgTypeId.DATE, PgTypeId.TIMESTAMP, PgTypeId.TIMESTAMPTZ],
    'Uint8Array': [PgTypeId.BYTEA]
}

export function filesEndingWith(dir: string, include: string[], exclude: string[], result: string[] = []) {
    const paths = fs.readdirSync(dir).map(f => path.join(dir, f))
    for (const p of paths) {
        if (fs.statSync(p).isDirectory()) {
            filesEndingWith(p, include, exclude, result)
        } else if (include.some(end => p.endsWith(end)) && !exclude.some(end => p.endsWith(end))) {
            result.push(p)
        }
    }
    return result
}

type FieldMap<T> = {
    [name: string]: {
        name: string
        dataType: T
        isNullable: boolean
    }
}

export function getTypeFields(typeChecker: ts.TypeChecker, typeNode: ts.TypeNode) {
    const typeFields: FieldMap<string> = {}

    typeChecker
        .getTypeFromTypeNode(typeNode)
        .getProperties()
        .map(prop => {
            const {valueDeclaration} = prop
            if (!valueDeclaration || !ts.isPropertySignature(valueDeclaration) || !valueDeclaration.type) {
                throw new Error(`Property "${typeNode.getText()}.${prop.getName()}" doesn't have a supported value declaration.`)
            }
            const rawType = typeChecker.typeToString(
                typeChecker.getTypeFromTypeNode(valueDeclaration.type)
            )
            const explicitlyOptional = !!(prop.flags & ts.SymbolFlags.Optional)
            const implicitlyOptional = rawType.endsWith(' | undefined') || rawType.endsWith(' | null')
            const dataType = implicitlyOptional ? rawType.substr(0, rawType.lastIndexOf(' | ')) : rawType

            typeFields[prop.getName()] = {
                name: prop.getName(),
                dataType,
                isNullable: explicitlyOptional || implicitlyOptional
            }
        })

    return typeFields
}

export async function scanFiles(fileNames: string[]) {
    const config = {
        host: 'localhost',
        port: 5432,
        database: 'pgeon_tmp_database',
        user: 'pgeon_tmp_user',
        password: 'pgeon_tmp_password'
    }

    const psqlAnon = (cmd: string) => execSync(`sudo -u postgres psql ${cmd}`)
    const psqlAuth = (cmd: string) => execSync(`sudo -u postgres psql "host=${config.host} port=${config.port} user=${config.user} dbname=${config.database} password='${config.password}'" ${cmd}`)

    psqlAnon(`-c "drop database if exists ${config.database}"`)
    psqlAnon(`-c "drop role if exists ${config.user}"`)
    psqlAnon(`-c "create role ${config.user} superuser login encrypted password '${config.password}'"`)
    psqlAnon(`-c "create database ${config.database} owner ${config.user} encoding 'UTF8'"`)
    psqlAuth(`-c "alter schema public owner to ${config.user}"`)

    const program = ts.createProgram(fileNames, {strictNullChecks: true})
    const typeChecker = program.getTypeChecker()

    // TODO Read config from environamental variables.
    // TODO For nullability, metadata tables must be read.
    const db = new pg.Client(config)
    db.connect()

    try {
        for (const fileName of fileNames) {
            await scanNode(
                program.getSourceFile(fileName)!,
                typeChecker,
                db
            )
        }
    } finally {
        db.end()

        psqlAnon(`-c "drop database if exists ${config.database}"`)
        psqlAnon(`-c "drop role if exists ${config.user}"`)
    }
}

export async function scanNode(node: ts.Node, typeChecker: ts.TypeChecker, db: pg.Client) {
    // TODO Improve regular expression, as the current one matches (e.g.) "$$query".
    if (ts.isTaggedTemplateExpression(node) && /\B\$query\b/.test(node.tag.getText())) {
        const {typeArguments} = node
        // FIXME This fails for `sample/02.ts`.
        if (!typeArguments || typeArguments.length !== 1) {
            throw new Error(`Unsupported number of generic types spotted in "$query" tagged template: ${typeArguments ? typeArguments.length : 0}.`)
        }

        const typeFields = getTypeFields(typeChecker, typeArguments[0])

        const {template} = node

        // TODO The query might not have any placeholders, in which case it will not be a template expression.
        if (!ts.isTemplateExpression(template)) {
            throw new Error('We lost track of the happy path!')
        }

        const query = [
            template.head.text,
            ...template.templateSpans.map(span => span.literal.text)
        ].join('null')

        try {
            const queryRes = await db.query(`select * from (${query}) x limit 0`)
            const queryFields: FieldMap<PgTypeId> = {}
            for (const queryField of queryRes.fields) {
                queryFields[queryField.name] = {
                    name: queryField.name,
                    dataType: queryField.dataTypeID,
                    isNullable: true
                }
            }

            for (const queryField of Object.values(queryFields)) {
                const typeField = typeFields[queryField.name]
                if (!typeField) {
                    throw new Error(`Field "${queryField.name}" was returned by the query but not declared in the return interface.`)
                }
                else {
                    const validJsTypes = (pgToJsType as any)[queryField.dataType]
                    if (!validJsTypes.includes(typeField.dataType)) {
                        throw new Error(`Field "${queryField.name}" was returned with type "${PgTypeId[queryField.dataType]}", but was declared with the incompatible type "${typeField.dataType}".`)
                    }
                }
            }

            for (const typeField of Object.values(typeFields)) {
                const queryField = queryFields[typeField.name]
                if (!queryField) {
                    throw new Error(`Field "${typeField.name}" was declared in the return interface but not declared by the query.`)
                }
                else {
                    const validPgTypes = (jsToPgType as any)[typeField.dataType]
                    if (!validPgTypes.includes(queryField.dataType)) {
                        throw new Error(`Field "${typeField.name}" was declared with type "${typeField.dataType}", but returned with the incompatible type "${PgTypeId[queryField.dataType]}".`)
                    }
                }
            }
        } catch (e) {
            console.error('Oops! We found a problem with your query!', e)
        }
    }

    for (const child of node.getChildren()) {
        await scanNode(child, typeChecker, db)
    }
}

// TODO Ensure double vertical spacing and four spaces of indentation.
// function errMsg(query: string, errPos?: number): string {
//     if (!errPos || errPos >= query.length) {
//         return '\n\x1b[31m' + query + '\x1b[0m\n'
//     }
//     return '\n\x1b[31m' + query.substr(0, errPos) + '\x1b[0m'
//         + '\x1b[103m' + '\x1b[31m' + query[errPos] + '\x1b[0m' + '\x1b[0m'
//         + '\x1b[31m' + query.substr(errPos + 1) + '\x1b[0m\n'
// }

// function nodeRef(node: ts.Node) {
//     const {line, character} = sourceFile.getLineAndCharacterOfPosition(node.getStart())
//     return `${sourceFile.fileName}:${line + 1}:${character + 1}`
// }
