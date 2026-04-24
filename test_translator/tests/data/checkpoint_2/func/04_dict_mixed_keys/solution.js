export function get_mixed_dict() {
    return new Map([
        [1, "int"],
        ["str", "string"],
        [[1, 2], "WRONG"]
    ]);
}
