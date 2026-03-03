add_subdirectory(backtest_core)
add_subdirectory(bindings)

if(AUTOBOT_BUILD_EXECUTOR)
  add_subdirectory(executor)
endif()
