#!/usr/bin/env node

import * as fs from 'fs'
import * as path from 'path'
import * as pg from 'pg'
import * as ts from 'typescript'

// import 'reflect-metadata'

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
    [PgTypeId.BOOL]: Boolean,
    [PgTypeId.INT2]: Number,
    [PgTypeId.INT4]: Number,
    [PgTypeId.INT8]: Number,
    [PgTypeId.FLOAT4]: Number,
    [PgTypeId.FLOAT8]: Number,
    [PgTypeId.NUMERIC]: Number,
    [PgTypeId.TEXT]: String,
    [PgTypeId.CHAR]: String,
    [PgTypeId.BPCHAR]: String,
    [PgTypeId.VARCHAR]: String,
    [PgTypeId.DATE]: Date,
    [PgTypeId.TIMESTAMP]: Date,
    [PgTypeId.TIMESTAMPTZ]: Date,
    [PgTypeId.BYTEA]: Uint8Array
}

function filesEndingWith(dir: string, ends: string[], result: string[] = []) {
    const paths = fs.readdirSync(dir).map(f => path.join(dir, f))
    for (const p of paths) {
        if (fs.statSync(p).isDirectory()) {
            filesEndingWith(p, ends, result)
        } else if (ends.some(end => p.endsWith(end))) {
            result.push(p)
        }
    }
    return result
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

const pool = new pg.Pool // TODO Auth.?

function getTypeObject(typeChecker: ts.TypeChecker, properties: ts.Symbol[]) {
    return properties.map(prop => ({
        field: prop.getName(),
        optional: !!(prop.flags & ts.SymbolFlags.Optional),
        type: typeChecker.typeToString(
            typeChecker.getTypeFromTypeNode(prop.valueDeclaration.type)
        )
    }))
}

function scanFile(sourceFile: ts.SourceFile, typeChecker: ts.TypeChecker) {
    scanNode(sourceFile)

    function scanNode(node: ts.Node) {
        // console.debug('node.kind', ts.SyntaxKind[node.kind])

        if (ts.isTaggedTemplateExpression(node)) {
            if (node.tag.expression.name.getText() !== '$query') {
                return
            }

            // console.debug('Tagged template node', node)
        }

        if (ts.isTypeNode(node) && node.getText() === 'User') {
            const userType = typeChecker.getTypeFromTypeNode(node)
            const props = userType.getProperties() // Alternatively: userType.members

            // getApparentProperties

            // props[1].valueDeclaration.type.kind === ts.SyntaxKind.StringKeyword
            // props[0].getDeclarations()[0] === props[0].valueDeclaration
            console.debug(
                'User',
                getTypeObject(typeChecker, props)
            )
        }

        /*
        if (node.kind === ts.SyntaxKind.Identifier
            && node.getText() === '$query'
            && ts.isTaggedTemplateExpression(node.parent)) {

            const parent = node.parent as ts.TaggedTemplateExpression
            const query = parent.template.getText().trim().slice(1, -1)

            pool.query(`select * from (${query}) x limit 0`).then(res => {
                // TODO Verify the function's declared return type and the actual return type match.
                console.debug(
                    res.fields.map(f => ({
                        name: f.name,
                        type: pgToJsType[f.dataTypeID as keyof typeof pgToJsType]
                    }))
                )

                // FIXME Gives `TypeError: Cannot read property 'exports' of undefined` from deep inside TypeScript.
                const signature = typeChecker.getResolvedSignature(parent)
                console.debug('signature', signature)

                // const type = typeChecker.getTypeFromTypeNode(node.typeArguments[0])
                // const properties = typeChecker.getPropertiesOfType(type)
                // typeChecker.typeToString(signature.getReturnType())
            })

            // Get query source.

            // let querySrc = parent.template.getText()
            // let query: string
            // while (true) {
            //     try {
            //         query = eval(querySrc)
            //         break
            //     } catch (err) {
            //         // TODO Any expression could be used inside the placeholders.
            //         if (err instanceof ReferenceError) {
            //             const undefinedVar = err.message.split(' ')[0]
            //             if (!/^[_a-zA-Z][_a-zA-Z0-9]+$/.test(undefinedVar)) {
            //                 console.warn(`Failed to capture undefined variable in query. Captured value: "${undefinedVar}".`)
            //                 return
            //             }
            //             querySrc = `let ${undefinedVar} = null; ` + querySrc
            //             continue
            //         }
            //         console.warn(`Failed to evaluate query source in ${nodeRef(node)}: ${err.message}`)
            //         return
            //     }
            // }

            // Look for syntatic errors.

            // const {query: ast, error: err} = pg.parse(query)
            // if (err) {
            //     console.error(
            //         `Syntactic error in ${nodeRef(node)}: ${err.message}`,
            //         errMsg(query, err.cursorPosition - 1)
            //     )
            //     return
            // }

            // Look for semantic and type errors.

            // pool.query('explain (verbose true) ' + query)
            //     .then(() => {
            //         if (ast[0].SelectStmt) {
            //             pool.query(`select * from (${query}) x limit 0`).then(res => {
            //                 // TODO Verify the function's declared return type and the actual return type match.
            //                 console.debug(
            //                     res.fields.map(f => ({
            //                         name: f.name,
            //                         type: pgToJsType[f.dataTypeID]
            //                     }))
            //                 )
            //
            //                 // FIXME Gives `TypeError: Cannot read property 'exports' of undefined` from deep inside TypeScript.
            //                 const signature = typeChecker.getResolvedSignature(parent)
            //                 console.debug('signature', signature)
            //
            //                 // const type = typeChecker.getTypeFromTypeNode(node.typeArguments[0])
            //                 // const properties = typeChecker.getPropertiesOfType(type)
            //                 // typeChecker.typeToString(signature.getReturnType())
            //             })
            //             .catch(console.error)
            //         }
            //     })
            //     .catch((err: Error & {position: number}) => {
            //         console.error(
            //             `Semantic error in ${nodeRef(node)}: ${err.message}`,
            //             errMsg(query, err.position - 1)
            //         )
            //         return
            //     })
        }
        */

        ts.forEachChild(node, scanNode)
    }

    // TODO Remove $HOME from filename.
    // function nodeRef(node: ts.Node) {
    //     const {line, character} = sourceFile.getLineAndCharacterOfPosition(node.getStart())
    //     return `${sourceFile.fileName}:${line + 1}:${character + 1}`
    // }
}

const args = process.argv.slice(2)
console.debug('args', args)

switch (args[0]) {

case 'help': {
    console.log(`
Usage:
    pgeon scan [<dir>]
`)
    process.exit(0)
    break
}

case 'scan': {
    const dir = args[1] || process.cwd()

    // TODO Scan .ts and .tsx files, but not .d.ts files.
    const fileNames = filesEndingWith(dir, ['example.ts'])
    const program = ts.createProgram(fileNames, {})
    const typeChecker = program.getTypeChecker()

    for (const fileName of fileNames) {
        scanFile(
            program.getSourceFile(fileName)!,
            typeChecker
        )
    }

    process.exit(0)
    break
}

// case 'fix': {
//     const dir = args[1] || process.cwd()
//     console.log('dir', dir)
//     process.exit(0)
//     break
// }

default: {
    console.log(`Unsupported command: "${args[0]}". Try \`pgeon help\`.`)
    process.exit(1)
    break
}

}
