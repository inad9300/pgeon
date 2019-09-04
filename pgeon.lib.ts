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

const jsToPgType: {[jsType: string]: PgTypeId[]} = {
  'boolean': [PgTypeId.BOOL],
  'Boolean': [PgTypeId.BOOL],
  'Uint8Array': [PgTypeId.BYTEA],
  'string': [PgTypeId.CHAR, PgTypeId.TEXT, PgTypeId.BPCHAR, PgTypeId.VARCHAR],
  'String': [PgTypeId.CHAR, PgTypeId.TEXT, PgTypeId.BPCHAR, PgTypeId.VARCHAR],
  'number': [PgTypeId.INT8, PgTypeId.INT2, PgTypeId.INT4, PgTypeId.FLOAT4, PgTypeId.FLOAT8, PgTypeId.NUMERIC],
  'Number': [PgTypeId.INT8, PgTypeId.INT2, PgTypeId.INT4, PgTypeId.FLOAT4, PgTypeId.FLOAT8, PgTypeId.NUMERIC],
  'BigInt': [PgTypeId.INT8, PgTypeId.INT2, PgTypeId.INT4, PgTypeId.FLOAT4, PgTypeId.FLOAT8, PgTypeId.NUMERIC],
  'Date': [PgTypeId.DATE, PgTypeId.TIMESTAMP, PgTypeId.TIMESTAMPTZ]
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
    [fieldName: string]: {
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
                throw new Error(`field "${typeNode.getText()}.${prop.getName()}" does not have a supported value declaration`)
            }
            const rawType = typeChecker.typeToString(
                typeChecker.getTypeFromTypeNode(valueDeclaration.type)
            )
            const explicitlyOptional = !!(prop.flags & ts.SymbolFlags.Optional)
            const implicitlyOptional = rawType.endsWith(' | undefined') || rawType.endsWith(' | null')
            const dataType = implicitlyOptional ? rawType.substr(0, rawType.lastIndexOf(' | ')) : rawType

            typeFields[prop.getName()] = {
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
    // TODO For nullability, metadata tables would need to be read... But not only columns are returned! :O
    const db = new pg.Client(config)
    db.connect()

    try {
        for (const fileName of fileNames) {
            const sourceFile = program.getSourceFile(fileName)!
            await scanNode(
                sourceFile,
                sourceFile,
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

export async function scanNode(node: ts.Node, sourceFile: ts.SourceFile, typeChecker: ts.TypeChecker, db: pg.Client) {
    if (ts.isTaggedTemplateExpression(node) && (node.tag.getText().endsWith('.$query') || node.tag.getText().includes('.$query<'))) {
        const {template} = node
        let query: string
        if (template.kind === ts.SyntaxKind.FirstTemplateToken) {
            query = template.text
        } else if (ts.isTemplateExpression(template)) {
            query = [
                template.head.text,
                ...template.templateSpans.map(span => span.literal.text)
            ].join('null')
        } else {
            throw new Error('found `$query` template tag with unsupported template kind')
        }

        const queryLines = query.replace(/^\n+/g, '').replace(/\s+$/g, '').split('\n')
        const minIndent = Math.min(...queryLines.map(ql => ql.length - ql.trimStart().length))
        const indentDiff = 4 - minIndent

        query = queryLines
            .map(ql => {
                if (indentDiff > 0) {
                    return ' '.repeat(indentDiff) + ql
                } else if (indentDiff < 0) {
                    return ql.substr(-1 * indentDiff)
                } else {
                    return ql
                }
            })
            .join('\n')

        const queryPrefix = 'select * from ('
        const querySuffix = ') x limit 0'

        let queryRes: pg.QueryResult
        try {
            queryRes = await db.query(queryPrefix + query + querySuffix)
        } catch (err) {
            console.error(errorMessage(sourceFile, node, err))
            console.error(errorQuery(query, !err.position ? undefined : err.position - queryPrefix.length - 1))
            return
        }

        const queryFields: FieldMap<PgTypeId> = {}
        for (const queryField of queryRes.fields) {
            queryFields[queryField.name] = {
                dataType: queryField.dataTypeID,
                isNullable: true
            }
        }

        const typeArguments = node.typeArguments || (node.tag as unknown as ts.NodeWithTypeArguments).typeArguments
        const typeArgument = typeArguments![0]
        const typeArgumentName = typeArgument.getText()
        const typeFields = getTypeFields(typeChecker, typeArgument)

        for (const queryFieldName of Object.keys(queryFields)) {
            const typeField = typeFields[queryFieldName]
            if (!typeField) {
                console.error(errorMessage(sourceFile, node, new Error(
                    `returned field "${queryFieldName}" was not declared in interface "${typeArgumentName}"`
                )))
                console.error(errorQuery(query))
            }
        }

        for (const [typeFieldName, typeField] of Object.entries(typeFields)) {
            const queryField = queryFields[typeFieldName]
            if (!queryField) {
                console.error(errorMessage(sourceFile, node, new Error(
                    `declared field "${typeArgumentName}.${typeFieldName}" was not returned by the query`
                )))
                console.error(errorQuery(query))
            }
            else {
                const validPgTypes = (jsToPgType as any)[typeField.dataType]
                if (!validPgTypes.includes(queryField.dataType)) {
                    console.error(errorMessage(sourceFile, node, new Error(
                        `type mismatch in "${typeArgumentName}.${typeFieldName}" – "${typeField.dataType}" and "${PgTypeId[queryField.dataType]}" are incompatible`
                    )))
                    console.error(errorQuery(query))
                }
            }
        }
    }

    for (const child of node.getChildren()) {
        await scanNode(child, sourceFile, typeChecker, db)
    }
}

function errorQuery(query: string, errPos?: number) {
    if (!errPos || errPos >= query.length) {
        return '\n\x1b[31m' + query + '\x1b[0m\n'
    } else {
        return '\n\x1b[31m' + query.substr(0, errPos) + '\x1b[0m'
            + '\x1b[41m' + '\x1b[30m' + query[errPos] + '\x1b[0m' + '\x1b[0m'
            + '\x1b[31m' + query.substr(errPos + 1) + '\x1b[0m\n'
    }
}

function errorMessage(sourceFile: ts.SourceFile, node: ts.Node, err: Error) {
    const {line} = sourceFile.getLineAndCharacterOfPosition(node.getStart())
    const errLocation = `${sourceFile.fileName}:${line + 1}`
    return `[\x1b[90m${errLocation}\x1b[0m] ${err.message}`
}
