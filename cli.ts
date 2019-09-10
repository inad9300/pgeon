#!/usr/bin/env node

import * as fs from 'fs'
import * as path from 'path'
import * as pg from 'pg'
import * as ts from 'typescript'

;(async () => {
    try {
        const args = process.argv.slice(2)
        switch (args[0]) {
            case 'help': {
                console.log(`
pgeon – Type checker for PostgreSQL queries written in TypeScript and node-posgres.

··· Commands ···

pgeon scan [<dir>] – Scan *.ts and *.tsx files in the given directory (or, by default, the current working directory) for type errors.
`)
                process.exit(0)
                break
            }
            case 'scan': {
                const dir = args[1] || process.cwd()
                await scanFiles(filesEndingWith(dir, ['.ts', '.tsx'], ['.d.ts']))

                process.exit(0)
                break
            }
            default: {
                console.error(`Unsupported command: "${args[0]}". Try running \`pgeon help\`.`)
                process.exit(1)
                break
            }
        }
    } catch (err) {
        if (typeof err === 'string') {
            console.error(`Error:`, err)
        } else {
            console.error(`Unexpected error:`, err)
        }
        process.exit(1)
    }
})()

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

async function scanFiles(fileNames: string[]) {
    const program = ts.createProgram(fileNames, {strictNullChecks: true})
    const typeChecker = program.getTypeChecker()

    const db = new pg.Client
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
    }
}

async function scanNode(node: ts.Node, sourceFile: ts.SourceFile, typeChecker: ts.TypeChecker, db: pg.Client) {
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
            throw 'found `$query` template tag with unsupported template kind'
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
            return console.error(
                queryError(err.message, sourceFile, node, query, !err.position ? undefined : err.position - queryPrefix.length - 1)
            )
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
                return console.error(
                    queryError(`returned field "${queryFieldName}" was not declared in interface "${typeArgumentName}"`, sourceFile, node, query)
                )
            }
        }

        for (const [typeFieldName, typeField] of Object.entries(typeFields)) {
            const queryField = queryFields[typeFieldName]
            if (!queryField) {
                return console.error(
                    queryError(`declared field "${typeArgumentName}.${typeFieldName}" was not returned by the query`, sourceFile, node, query)
                )
            }
            else {
                const validPgTypes = (jsToPgType as any)[typeField.dataType]
                if (!validPgTypes.includes(queryField.dataType)) {
                    return console.error(
                        queryError(`type mismatch in "${typeArgumentName}.${typeFieldName}" – "${typeField.dataType}" and "${PgTypeId[queryField.dataType]}" are incompatible`, sourceFile, node, query)
                    )
                }
            }
        }
    }

    for (const child of node.getChildren()) {
        await scanNode(child, sourceFile, typeChecker, db)
    }
}

type FieldMap<T extends string | number> = {
    [fieldName: string]: {
        dataType: T
        isNullable: boolean
    }
}

function getTypeFields(typeChecker: ts.TypeChecker, typeNode: ts.TypeNode) {
    const typeFields: FieldMap<string> = {}

    typeChecker
        .getTypeFromTypeNode(typeNode)
        .getProperties()
        .map(prop => {
            const {valueDeclaration} = prop
            if (!valueDeclaration || !ts.isPropertySignature(valueDeclaration) || !valueDeclaration.type) {
                throw `field "${typeNode.getText()}.${prop.getName()}" does not have a supported value declaration`
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

function queryError(
    errMessage: string,
    sourceFile: ts.SourceFile,
    node: ts.Node,
    query: string,
    errPosition?: number
): string {
    const {line} = sourceFile.getLineAndCharacterOfPosition(node.getStart())
    const errLocation = `${sourceFile.fileName}:${line + 1}`
    const str = `[\x1b[90m${errLocation}\x1b[0m] ${errMessage}`

    if (errPosition === undefined || errPosition >= query.length) {
        return str + '\n\x1b[31m' + query + '\x1b[0m\n'
    } else {
        return str + '\n\x1b[31m' + query.substr(0, errPosition) + '\x1b[0m'
            + '\x1b[41m' + '\x1b[30m' + query[errPosition] + '\x1b[0m' + '\x1b[0m'
            + '\x1b[31m' + query.substr(errPosition + 1) + '\x1b[0m\n'
    }
}

function filesEndingWith(dir: string, include: string[], exclude: string[], result: string[] = []) {
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
