export function get_mixed_dict(): Map<number | string | number[], string> {
    return new Map([
        [1, "int"],
        ["str", "string"],
        [[1, 2], "tuple"]
    ]);
}
