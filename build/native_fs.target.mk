# This file is generated by gyp; do not edit.

TOOLSET := target
TARGET := native_fs
DEFS_Debug := \
	'-DNODE_GYP_MODULE_NAME=native_fs' \
	'-D_LARGEFILE_SOURCE' \
	'-D_FILE_OFFSET_BITS=64' \
	'-DBUILDING_NODE_EXTENSION' \
	'-DDEBUG' \
	'-D_DEBUG'

# Flags passed to all source files.
CFLAGS_Debug := \
	-fPIC \
	-pthread \
	-Wall \
	-Wextra \
	-Wno-unused-parameter \
	-m64 \
	-g \
	-O0

# Flags passed to only C files.
CFLAGS_C_Debug :=

# Flags passed to only C++ files.
CFLAGS_CC_Debug := \
	-fno-rtti \
	-fno-exceptions \
	-std=gnu++0x

INCS_Debug := \
	-I/home/patrick/.node-gyp/5.1.0/include/node \
	-I/home/patrick/.node-gyp/5.1.0/src \
	-I/home/patrick/.node-gyp/5.1.0/deps/uv/include \
	-I/home/patrick/.node-gyp/5.1.0/deps/v8/include

DEFS_Release := \
	'-DNODE_GYP_MODULE_NAME=native_fs' \
	'-D_LARGEFILE_SOURCE' \
	'-D_FILE_OFFSET_BITS=64' \
	'-DBUILDING_NODE_EXTENSION'

# Flags passed to all source files.
CFLAGS_Release := \
	-fPIC \
	-pthread \
	-Wall \
	-Wextra \
	-Wno-unused-parameter \
	-m64 \
	-O3 \
	-ffunction-sections \
	-fdata-sections \
	-fno-omit-frame-pointer

# Flags passed to only C files.
CFLAGS_C_Release :=

# Flags passed to only C++ files.
CFLAGS_CC_Release := \
	-fno-rtti \
	-fno-exceptions \
	-std=gnu++0x

INCS_Release := \
	-I/home/patrick/.node-gyp/5.1.0/include/node \
	-I/home/patrick/.node-gyp/5.1.0/src \
	-I/home/patrick/.node-gyp/5.1.0/deps/uv/include \
	-I/home/patrick/.node-gyp/5.1.0/deps/v8/include

OBJS := \
	$(obj).target/$(TARGET)/main.o

# Add to the list of files we specially track dependencies for.
all_deps += $(OBJS)

# CFLAGS et al overrides must be target-local.
# See "Target-specific Variable Values" in the GNU Make manual.
$(OBJS): TOOLSET := $(TOOLSET)
$(OBJS): GYP_CFLAGS := $(DEFS_$(BUILDTYPE)) $(INCS_$(BUILDTYPE))  $(CFLAGS_$(BUILDTYPE)) $(CFLAGS_C_$(BUILDTYPE))
$(OBJS): GYP_CXXFLAGS := $(DEFS_$(BUILDTYPE)) $(INCS_$(BUILDTYPE))  $(CFLAGS_$(BUILDTYPE)) $(CFLAGS_CC_$(BUILDTYPE))

# Suffix rules, putting all outputs into $(obj).

$(obj).$(TOOLSET)/$(TARGET)/%.o: $(srcdir)/%.cc FORCE_DO_CMD
	@$(call do_cmd,cxx,1)

# Try building from generated source, too.

$(obj).$(TOOLSET)/$(TARGET)/%.o: $(obj).$(TOOLSET)/%.cc FORCE_DO_CMD
	@$(call do_cmd,cxx,1)

$(obj).$(TOOLSET)/$(TARGET)/%.o: $(obj)/%.cc FORCE_DO_CMD
	@$(call do_cmd,cxx,1)

# End of this set of suffix rules
### Rules for final target.
LDFLAGS_Debug := \
	-pthread \
	-rdynamic \
	-m64

LDFLAGS_Release := \
	-pthread \
	-rdynamic \
	-m64

LIBS :=

$(obj).target/native_fs.node: GYP_LDFLAGS := $(LDFLAGS_$(BUILDTYPE))
$(obj).target/native_fs.node: LIBS := $(LIBS)
$(obj).target/native_fs.node: TOOLSET := $(TOOLSET)
$(obj).target/native_fs.node: $(OBJS) FORCE_DO_CMD
	$(call do_cmd,solink_module)

all_deps += $(obj).target/native_fs.node
# Add target alias
.PHONY: native_fs
native_fs: $(builddir)/native_fs.node

# Copy this to the executable output path.
$(builddir)/native_fs.node: TOOLSET := $(TOOLSET)
$(builddir)/native_fs.node: $(obj).target/native_fs.node FORCE_DO_CMD
	$(call do_cmd,copy)

all_deps += $(builddir)/native_fs.node
# Short alias for building this executable.
.PHONY: native_fs.node
native_fs.node: $(obj).target/native_fs.node $(builddir)/native_fs.node

# Add executable to "all" target.
.PHONY: all
all: $(builddir)/native_fs.node

