#!/usr/bin/env node
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const commander_1 = require("commander");
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const sync_1 = require("csv-parse/sync");
const sync_2 = require("csv-stringify/sync");
const program = new commander_1.Command();
program
    .name('jsonforge')
    .description('A powerful JSON/CSV converter with transformations')
    .version('1.0.0');
program
    .command('convert')
    .description('Convert between JSON and CSV formats')
    .argument('<input>', 'Input file (use - for stdin)')
    .argument('<output>', 'Output file (use - for stdout)')
    .option('-f, --format <format>', 'Force output format: json or csv', '')
    .option('-k, --key <key>', 'For JSON array of objects, extract nested key')
    .option('-p, --pretty', 'Pretty print JSON output', false)
    .action(async (input, output, options) => {
    try {
        let data;
        // Read input
        if (input === '-') {
            data = JSON.parse(await readStdin());
        }
        else {
            const ext = path.extname(input).toLowerCase();
            const fileContent = fs.readFileSync(input, 'utf-8');
            if (ext === '.json' || ext === '.jsonl') {
                data = JSON.parse(fileContent);
            }
            else if (ext === '.csv') {
                data = (0, sync_1.parse)(fileContent, { columns: true, skip_empty_lines: true });
            }
            else {
                // Try to auto-detect
                try {
                    data = JSON.parse(fileContent);
                }
                catch {
                    data = (0, sync_1.parse)(fileContent, { columns: true, skip_empty_lines: true });
                }
            }
        }
        // Extract nested key if specified
        if (options.key) {
            const keys = options.key.split('.');
            data = keys.reduce((obj, k) => obj?.[k], data);
        }
        // Determine output format
        const inputIsJson = !Array.isArray(data) || typeof data[0] === 'object';
        const outputFormat = options.format || (Array.isArray(data) && typeof data[0] === 'object' ? 'csv' : 'json');
        let result;
        if (outputFormat === 'csv' || (outputFormat === '' && inputIsJson && Array.isArray(data))) {
            // Convert JSON to CSV
            if (!Array.isArray(data))
                data = [data];
            result = (0, sync_2.stringify)(data, { header: true });
        }
        else {
            // Convert CSV to JSON
            if (!Array.isArray(data))
                data = [data];
            result = options.pretty ? JSON.stringify(data, null, 2) : JSON.stringify(data);
        }
        // Write output
        if (output === '-') {
            console.log(result);
        }
        else {
            fs.writeFileSync(output, result);
            console.log(`Converted ${input} -> ${output}`);
        }
    }
    catch (error) {
        console.error(`Error: ${error.message}`);
        process.exit(1);
    }
});
program
    .command('transform')
    .description('Transform JSON/CSV data with filters, selects, and aggregations')
    .argument('<input>', 'Input file (use - for stdin)')
    .argument('<output>', 'Output file (use - for stdout)')
    .option('-q, --query <jsonpath>', 'Filter using JSONPath-like expression')
    .option('-s, --select <fields>', 'Comma-separated fields to select')
    .option('--sort <field>', 'Sort by field')
    .option('--order <order>', 'Sort order: asc or desc', 'asc')
    .option('-l, --limit <n>', 'Limit number of results', (v) => parseInt(v))
    .option('--group-by <field>', 'Group by field and aggregate')
    .option('--aggregate <agg>', 'Aggregation: count,sum,avg,min,max')
    .option('--having <condition>', 'Having clause (e.g., "count>5")')
    .option('-f, --format <format>', 'Output format: json or csv', 'json')
    .option('-p, --pretty', 'Pretty print JSON', false)
    .action(async (input, output, options) => {
    try {
        let data;
        // Read input
        if (input === '-') {
            const content = await readStdin();
            try {
                data = JSON.parse(content);
            }
            catch {
                data = (0, sync_1.parse)(content, { columns: true, skip_empty_lines: true });
            }
        }
        else {
            const ext = path.extname(input).toLowerCase();
            const fileContent = fs.readFileSync(input, 'utf-8');
            if (ext === '.csv') {
                data = (0, sync_1.parse)(fileContent, { columns: true, skip_empty_lines: true });
            }
            else {
                data = JSON.parse(fileContent);
                if (!Array.isArray(data))
                    data = [data];
            }
        }
        // Apply filter/query
        if (options.query) {
            data = filterData(data, options.query);
        }
        // Apply sort
        if (options.sort) {
            const field = options.sort;
            const order = options.order === 'desc' ? -1 : 1;
            data.sort((a, b) => {
                if (a[field] < b[field])
                    return -1 * order;
                if (a[field] > b[field])
                    return 1 * order;
                return 0;
            });
        }
        // Apply limit
        if (options.limit) {
            data = data.slice(0, options.limit);
        }
        // Apply group by
        if (options.groupBy) {
            const groups = new Map();
            for (const row of data) {
                const key = row[options.groupBy];
                if (!groups.has(key))
                    groups.set(key, []);
                groups.get(key).push(row);
            }
            const aggregated = [];
            for (const [key, rows] of groups) {
                const result = { [options.groupBy]: key };
                const numericFields = getNumericFields(rows);
                for (const field of numericFields) {
                    const values = rows.map((r) => r[field]).filter((v) => v != null);
                    switch (options.aggregate || 'count') {
                        case 'count':
                            result[`${field}_count`] = values.length;
                            break;
                        case 'sum':
                            result[`${field}_sum`] = values.reduce((a, b) => a + b, 0);
                            break;
                        case 'avg':
                            result[`${field}_avg`] = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
                            break;
                        case 'min':
                            result[`${field}_min`] = Math.min(...values);
                            break;
                        case 'max':
                            result[`${field}_max`] = Math.max(...values);
                            break;
                    }
                }
                aggregated.push(result);
            }
            // Apply having
            if (options.having) {
                const match = options.having.match(/(\w+)([><=]+)(\d+)/);
                if (match) {
                    const [, field, op, val] = match;
                    const numVal = parseFloat(val);
                    data = aggregated.filter((r) => {
                        const fieldVal = r[field.replace('_count', '_count').replace('_sum', '_sum').replace('_avg', '_avg')];
                        switch (op) {
                            case '>': return fieldVal > numVal;
                            case '<': return fieldVal < numVal;
                            case '>=': return fieldVal >= numVal;
                            case '<=': return fieldVal <= numVal;
                            case '=': return fieldVal === numVal;
                            default: return true;
                        }
                    });
                }
            }
            else {
                data = aggregated;
            }
        }
        else {
            // Select specific fields
            if (options.select) {
                const fields = options.select.split(',').map((f) => f.trim());
                data = data.map((row) => {
                    const selected = {};
                    for (const field of fields) {
                        if (field in row)
                            selected[field] = row[field];
                    }
                    return selected;
                });
            }
        }
        // Format output
        let result;
        if (options.format === 'csv' || options.f === 'csv') {
            result = (0, sync_2.stringify)(data, { header: true });
        }
        else {
            result = options.pretty ? JSON.stringify(data, null, 2) : JSON.stringify(data);
        }
        if (output === '-') {
            console.log(result);
        }
        else {
            fs.writeFileSync(output, result);
            console.log(`Transformed ${input} -> ${output} (${data.length} rows)`);
        }
    }
    catch (error) {
        console.error(`Error: ${error.message}`);
        process.exit(1);
    }
});
program
    .command('pivot')
    .description('Pivot JSON/CSV data')
    .argument('<input>', 'Input file')
    .argument('<output>', 'Output file')
    .option('-r, --rows <field>', 'Field to use for row groups')
    .option('-c, --cols <field>', 'Field to use for column headers')
    .option('-v, --values <field>', 'Field to aggregate')
    .option('-a, --agg <type>', 'Aggregation type: sum,count,avg', 'sum')
    .action(async (input, output, options) => {
    try {
        let data;
        const ext = path.extname(input).toLowerCase();
        const fileContent = fs.readFileSync(input, 'utf-8');
        if (ext === '.csv') {
            data = (0, sync_1.parse)(fileContent, { columns: true, skip_empty_lines: true });
        }
        else {
            data = JSON.parse(fileContent);
            if (!Array.isArray(data))
                data = [data];
        }
        if (!options.rows || !options.cols || !options.values) {
            console.error('Error: --rows, --cols, and --values are required');
            process.exit(1);
        }
        // Build pivot table
        const pivot = {};
        const colValues = new Set();
        for (const row of data) {
            const rowKey = row[options.rows];
            const colKey = row[options.cols];
            const val = parseFloat(row[options.values]) || 0;
            colValues.add(colKey);
            if (!pivot[rowKey])
                pivot[rowKey] = {};
            pivot[rowKey][colKey] = (pivot[rowKey][colKey] || 0) + val;
        }
        // Convert to array format
        const result = [];
        for (const [rowKey, cols] of Object.entries(pivot)) {
            const entry = { [options.rows]: rowKey };
            for (const colKey of colValues) {
                entry[colKey] = cols[colKey] || 0;
            }
            result.push(entry);
        }
        const outputData = options.format === 'csv' ? (0, sync_2.stringify)(result, { header: true }) : JSON.stringify(result, null, 2);
        if (output === '-') {
            console.log(outputData);
        }
        else {
            fs.writeFileSync(output, outputData);
            console.log(`Pivoted ${input} -> ${output}`);
        }
    }
    catch (error) {
        console.error(`Error: ${error.message}`);
        process.exit(1);
    }
});
program
    .command('stats')
    .description('Show statistics for JSON/CSV data')
    .argument('<input>', 'Input file')
    .option('-f, --field <field>', 'Calculate stats for specific field')
    .action(async (input, options) => {
    try {
        let data;
        const ext = path.extname(input).toLowerCase();
        const fileContent = fs.readFileSync(input, 'utf-8');
        if (ext === '.csv') {
            data = (0, sync_1.parse)(fileContent, { columns: true, skip_empty_lines: true });
        }
        else {
            data = JSON.parse(fileContent);
            if (!Array.isArray(data))
                data = [data];
        }
        console.log(`\n📊 Statistics for ${input}`);
        console.log(`   Rows: ${data.length}`);
        console.log(`   Columns: ${Object.keys(data[0] || {}).length}`);
        if (options.field && data.length > 0 && options.field in data[0]) {
            const field = options.field;
            const values = data.map((r) => parseFloat(r[field])).filter((v) => !isNaN(v));
            if (values.length > 0) {
                const sum = values.reduce((a, b) => a + b, 0);
                const avg = sum / values.length;
                const sorted = [...values].sort((a, b) => a - b);
                const min = sorted[0];
                const max = sorted[sorted.length - 1];
                const median = sorted.length % 2 === 0 ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2 : sorted[Math.floor(sorted.length / 2)];
                console.log(`\n   Field: ${options.field}`);
                console.log(`   Count: ${values.length}`);
                console.log(`   Sum: ${sum.toFixed(2)}`);
                console.log(`   Avg: ${avg.toFixed(2)}`);
                console.log(`   Min: ${min.toFixed(2)}`);
                console.log(`   Max: ${max.toFixed(2)}`);
                console.log(`   Median: ${median.toFixed(2)}`);
            }
        }
        else if (data.length > 0) {
            console.log(`\n   Columns: ${Object.keys(data[0]).join(', ')}`);
        }
        console.log('');
    }
    catch (error) {
        console.error(`Error: ${error.message}`);
        process.exit(1);
    }
});
function readStdin() {
    return new Promise((resolve) => {
        let data = '';
        process.stdin.on('data', (chunk) => data += chunk);
        process.stdin.on('end', () => resolve(data));
    });
}
function filterData(data, query) {
    // Simple JSONPath-like filtering
    // Supports: $.field, $[field=value], $[field>value]
    const equalMatch = query.match(/\$\[(\w+)=(.+)\]/);
    if (equalMatch) {
        const [, field, value] = equalMatch;
        return data.filter(row => String(row[field]) === value);
    }
    const gtMatch = query.match(/\$\[(\w+)>(.+)\]/);
    if (gtMatch) {
        const [, field, value] = gtMatch;
        const numVal = parseFloat(value);
        return data.filter(row => parseFloat(row[field]) > numVal);
    }
    const ltMatch = query.match(/\$\[(\w+)<(.+)\]/);
    if (ltMatch) {
        const [, field, value] = ltMatch;
        const numVal = parseFloat(value);
        return data.filter(row => parseFloat(row[field]) < numVal);
    }
    // Simple field access
    const fieldMatch = query.match(/\$\.(\w+)/);
    if (fieldMatch) {
        return data.map(row => row[fieldMatch[1]]);
    }
    return data;
}
function getNumericFields(data) {
    if (data.length === 0)
        return [];
    const fields = [];
    for (const [key, value] of Object.entries(data[0])) {
        if (typeof value === 'number' || data.some(row => typeof row[key] === 'number')) {
            fields.push(key);
        }
    }
    return fields;
}
program.parse();
