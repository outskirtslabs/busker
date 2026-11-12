const std = @import("std");

const TargetConfig = struct {
    query: std.Target.Query,
    dir_name: []const u8,
    lib_name: []const u8,
};

pub fn build(b: *std.Build) void {
    const all_targets = b.option(bool, "all-targets", "Build for all supported platforms") orelse true;
    const optimize = b.standardOptimizeOption(.{});
    const native_target = b.standardTargetOptions(.{});
    var patched = std.AutoHashMap(*std.Build.Step.Compile, void).init(b.allocator);

    const all_target_configs = [_]TargetConfig{
        .{
            .query = .{ .cpu_arch = .x86_64, .os_tag = .linux },
            .dir_name = "linux-x86-64",
            .lib_name = "libh2oclj.so",
        },
        .{
            .query = .{ .cpu_arch = .aarch64, .os_tag = .linux },
            .dir_name = "linux-aarch64",
            .lib_name = "libh2oclj.so",
        },
        .{
            .query = .{ .cpu_arch = .x86_64, .os_tag = .macos },
            .dir_name = "macos-x86-64",
            .lib_name = "libh2oclj.dylib",
        },
        .{
            .query = .{ .cpu_arch = .aarch64, .os_tag = .macos },
            .dir_name = "macos-aarch64",
            .lib_name = "libh2oclj.dylib",
        },
    };

    if (all_targets) {
        const native = native_target.result;
        const native_arch = native.cpu.arch;
        const native_os = native.os.tag;

        for (all_target_configs) |target_config| {
            const is_native = (target_config.query.cpu_arch == native_arch and target_config.query.os_tag == native_os);

            if (is_native) {
                const native_config = TargetConfig{
                    .query = .{ .cpu_model = .baseline },
                    .dir_name = target_config.dir_name,
                    .lib_name = target_config.lib_name,
                };
                buildShimForTarget(b, native_config, optimize, &patched);
            } else {
                buildShimForTarget(b, target_config, optimize, &patched);
            }
        }
    } else {
        const native = native_target.result;
        const native_arch = native.cpu.arch;
        const native_os = native.os.tag;

        var matched_config: ?TargetConfig = null;
        for (all_target_configs) |target_config| {
            if (target_config.query.cpu_arch == native_arch and target_config.query.os_tag == native_os) {
                matched_config = TargetConfig{
                    .query = .{ .cpu_model = .baseline },
                    .dir_name = target_config.dir_name,
                    .lib_name = target_config.lib_name,
                };
                break;
            }
        }

        const native_config = matched_config orelse {
            @panic("Unsupported native platform - add it to all_target_configs");
        };

        buildShimForTarget(b, native_config, optimize, &patched);
    }

    const test_h2o = patchedH2o(b, b.dependency("h2o-zig", .{
        .target = b.graph.host,
        .optimize = optimize,
    }), &patched);
    const test_step = b.step("test", "Test native callbacks and fixed-slot lifetime");
    const native_tests = [_][]const u8{ "quic_forwarding", "address_tokens", "fixed_retention", "socket_write_completion" };
    for (native_tests) |test_name| {
        const forwarding_test = b.addExecutable(.{
            .name = b.fmt("test-{s}", .{test_name}),
            .root_module = b.createModule(.{
                .target = b.graph.host,
                .optimize = optimize,
                .link_libc = true,
            }),
        });
        forwarding_test.root_module.addCSourceFile(.{
            .file = b.path(b.fmt("../src/shim/test_{s}.c", .{test_name})),
            .flags = &.{ "-std=c17", "-D_GNU_SOURCE", "-DH2O_USE_LIBUV=0", "-DCLJ_DEBUG=0" },
        });
        forwarding_test.root_module.addIncludePath(test_h2o.getEmittedIncludeTree());
        forwarding_test.root_module.linkLibrary(test_h2o);
        forwarding_test.root_module.linkSystemLibrary("pthread", .{});
        forwarding_test.root_module.linkSystemLibrary("dl", .{});
        forwarding_test.root_module.linkSystemLibrary("m", .{});
        const run_forwarding_test = b.addRunArtifact(forwarding_test);
        if (std.mem.eql(u8, test_name, "address_tokens")) {
            run_forwarding_test.addFileArg(b.path("../src/test/fixtures/server.crt"));
            run_forwarding_test.addFileArg(b.path("../src/test/fixtures/server.key"));
        }
        test_step.dependOn(&run_forwarding_test.step);
        b.getInstallStep().dependOn(&run_forwarding_test.step);
    }

    const compile_flags_step = b.step("compile-flags", "Generate compile_flags.txt for LSP");
    const compile_flags_cmd = b.addSystemCommand(&[_][]const u8{ "sh", "-c" });

    const is_debug = optimize == .debug;
    const native_os = native_target.result.os.tag;
    const is_linux = native_os == .linux;
    const is_macos = native_os == .macos;

    const debug_flag = if (is_debug) "-DCLJ_DEBUG=1" else "-DCLJ_DEBUG=0";
    const os_flag = if (is_linux) "-D_GNU_SOURCE" else if (is_macos) "-D_DARWIN_C_SOURCE" else "";

    const cwd = b.root.root_dir.handle.realPathFileAlloc(b.graph.io, ".", b.allocator) catch @panic("cannot resolve shim directory");
    const compile_flags_script = std.fmt.allocPrint(b.allocator,
        \\echo '# Auto-generated by Zig build - do not edit' > ../src/shim/compile_flags.txt && \
        \\echo '-std=c17' >> ../src/shim/compile_flags.txt && \
        \\echo '-Wall' >> ../src/shim/compile_flags.txt && \
        \\echo '-Wextra' >> ../src/shim/compile_flags.txt && \
        \\echo '-Wno-null-pointer-subtraction' >> ../src/shim/compile_flags.txt && \
        \\echo '-Wno-missing-field-initializers' >> ../src/shim/compile_flags.txt && \
        \\echo '-D_POSIX_C_SOURCE=200809L' >> ../src/shim/compile_flags.txt && \
        \\echo '-DH2O_USE_LIBUV=0' >> ../src/shim/compile_flags.txt && \
        \\echo '{s}' >> ../src/shim/compile_flags.txt && \
        \\echo '{s}' >> ../src/shim/compile_flags.txt && \
        \\echo '-I{s}/../src/shim' >> ../src/shim/compile_flags.txt && \
        \\echo '-I{s}/../h2o-zig/zig-out/include' >> ../src/shim/compile_flags.txt
    , .{ debug_flag, os_flag, cwd, cwd }) catch unreachable;

    compile_flags_cmd.addArg(compile_flags_script);
    compile_flags_step.dependOn(&compile_flags_cmd.step);
}

// Reconcile write interest before completion callbacks: https://github.com/h2o/h2o/issues/3828
// Remove this patch when the pinned H2O includes the repair.
fn patchedH2o(b: *std.Build, dep: *std.Build.Dependency, patched: *std.AutoHashMap(*std.Build.Step.Compile, void)) *std.Build.Step.Compile {
    const lib = dep.artifact("h2o-evloop");
    const entry = patched.getOrPut(lib) catch @panic("OOM");
    if (entry.found_existing) return lib;

    const h2o = dep.builder.dependency("h2o", .{});
    const header_path = "lib/common/socket/evloop.c.h";
    b.dependOnFileContents(h2o.path(header_path));
    var source_dir = h2o.builder.root.openDir(b.graph.io, ".", .{}) catch @panic("cannot open H2O source directory");
    defer source_dir.close(b.graph.io);
    const original = source_dir.readFileAlloc(b.graph.io, header_path, b.allocator, .limited(1024 * 1024)) catch
        @panic("cannot read H2O event-loop header");
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(original, &digest, .{});
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), "cdb91cd4d4f6d4c95a1815e79343bf922898cbf6ca8715d8d4bd2b4d088d64f3"))
        @panic("H2O event-loop header changed; review the write-completion patch");
    const before = "        on_write_complete(&sock->super, err);";
    const pos = std.mem.indexOf(u8, original, before) orelse @panic("H2O completion patch site missing");
    if (std.mem.indexOfPos(u8, original, pos + before.len, before) != null)
        @panic("H2O completion patch site is not unique");
    const contents = b.fmt("{s}        link_to_statechanged(sock);\n{s}", .{ original[0..pos], original[pos..] });
    const files = b.addWriteFiles();
    _ = files.add("socket/evloop.c.h", contents);
    substituteSource(lib, h2o, "lib/common/socket.c", files.addCopyFile(h2o.path("lib/common/socket.c"), "socket.c"));

    // Relative includes other than the patched header still use the pinned source tree.
    lib.root_module.addIncludePath(h2o.path("lib/common"));
    lib.root_module.addIncludePath(h2o.path("lib/common/socket"));
    lib.step.dependOn(&files.step);
    return lib;
}

fn substituteSource(lib: *std.Build.Step.Compile, h2o: *std.Build.Dependency, sub_path: []const u8, file: std.Build.LazyPath) void {
    var matches: usize = 0;
    for (lib.root_module.link_objects.items) |object| {
        if (object == .c_source_file) {
            const source = object.c_source_file;
            if (source.file == .dependency and source.file.dependency.dependency == h2o and
                std.mem.eql(u8, source.file.dependency.sub_path, sub_path))
            {
                source.file = file;
                matches += 1;
            }
        }
    }
    if (matches != 1) @panic("expected exactly one matching H2O translation unit");
}

fn buildShimForTarget(b: *std.Build, target_config: TargetConfig, optimize: std.builtin.OptimizeMode, patched: *std.AutoHashMap(*std.Build.Step.Compile, void)) void {
    const target = b.resolveTargetQuery(target_config.query);
    const is_debug = optimize == .debug;
    const is_linux = target.result.os.tag == .linux;
    const is_macos = target.result.os.tag == .macos;

    const h2o_dep = b.dependency("h2o-zig", .{
        .target = target,
        .optimize = optimize,
    });

    const artifact_name = std.fmt.allocPrint(
        b.allocator,
        "h2oclj-{s}",
        .{target_config.dir_name},
    ) catch unreachable;

    const shim = b.addLibrary(.{
        .name = artifact_name,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
        }),
        .linkage = .dynamic,
    });

    shim.link_gc_sections = false;

    const shim_cflags = blk: {
        var flags = std.ArrayList([]const u8).initCapacity(b.allocator, 10) catch unreachable;
        flags.append(b.allocator, "-std=c17") catch unreachable;
        flags.append(b.allocator, "-Wall") catch unreachable;
        flags.append(b.allocator, "-Wextra") catch unreachable;
        flags.append(b.allocator, "-Wno-null-pointer-subtraction") catch unreachable;
        flags.append(b.allocator, "-Wno-missing-field-initializers") catch unreachable;
        flags.append(b.allocator, "-D_POSIX_C_SOURCE=200809L") catch unreachable;
        flags.append(b.allocator, "-DH2O_USE_LIBUV=0") catch unreachable;

        if (is_debug) {
            flags.append(b.allocator, "-DCLJ_DEBUG=1") catch unreachable;
        } else {
            flags.append(b.allocator, "-DCLJ_DEBUG=0") catch unreachable;
        }

        if (is_linux) {
            flags.append(b.allocator, "-D_GNU_SOURCE") catch unreachable;
        } else if (is_macos) {
            flags.append(b.allocator, "-D_DARWIN_C_SOURCE") catch unreachable;
        }

        break :blk flags.toOwnedSlice(b.allocator) catch unreachable;
    };

    shim.root_module.addCSourceFile(.{
        .file = b.path("../src/shim/shim.c"),
        .flags = shim_cflags,
    });

    shim.root_module.addIncludePath(b.path("../src/shim"));

    const h2o_lib = patchedH2o(b, h2o_dep, patched);
    shim.root_module.addIncludePath(h2o_lib.getEmittedIncludeTree());

    shim.root_module.linkLibrary(h2o_lib);

    shim.root_module.link_libc = true;
    shim.root_module.linkSystemLibrary("pthread", .{});
    shim.root_module.linkSystemLibrary("dl", .{});
    shim.root_module.linkSystemLibrary("m", .{});

    const install_path = std.fmt.allocPrint(
        b.allocator,
        "{s}/{s}",
        .{ target_config.dir_name, target_config.lib_name },
    ) catch unreachable;

    const install_lib = b.addInstallFileWithDir(
        shim.getEmittedBin(),
        .prefix,
        install_path,
    );
    b.getInstallStep().dependOn(&install_lib.step);

    const mkdir_cmd = b.addSystemCommand(&[_][]const u8{
        "mkdir",
        "-p",
    });
    mkdir_cmd.addArg(std.fmt.allocPrint(
        b.allocator,
        "{s}/resources/{s}",
        .{ target_config.dir_name, target_config.dir_name },
    ) catch unreachable);
    mkdir_cmd.step.dependOn(&install_lib.step);

    const cp_cmd = b.addSystemCommand(&[_][]const u8{
        "cp",
        "-v",
    });
    cp_cmd.addArg(std.fmt.allocPrint(
        b.allocator,
        "zig-out/{s}/{s}",
        .{ target_config.dir_name, target_config.lib_name },
    ) catch unreachable);
    cp_cmd.addArg(std.fmt.allocPrint(
        b.allocator,
        "{s}/resources/{s}/",
        .{ target_config.dir_name, target_config.dir_name },
    ) catch unreachable);
    cp_cmd.step.dependOn(&mkdir_cmd.step);
    b.getInstallStep().dependOn(&cp_cmd.step);

    shim.installHeader(b.path("../src/shim/shim.h"), "shim.h");
}
